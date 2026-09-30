package com.wso2cre.asyncdemo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.apache.synapse.commons.json.JsonUtil;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.core.axis2.Axis2Sender;
import org.apache.synapse.message.MessageConsumer;
import org.apache.synapse.message.MessageProducer;
import org.apache.synapse.message.store.AbstractMessageStore;
import org.apache.synapse.message.store.Constants;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A Message Store implementation deployed and used exactly like the stock in-memory store
 * (`<messageStore name="X" class="com.wso2cre.asyncdemo.ExpiringConcurrentMapMessageStore">`,
 * parked via the stock `<store messageStore="X"/>` mediator) but backed by a ConcurrentHashMap
 * keyed directly by an application-chosen correlation id, instead of InMemoryStore's
 * ConcurrentLinkedQueue + single global lock + linear scan on get(id)/remove(id)/getAll().
 *
 * <p>Confirmed from source (org.apache.synapse.message.store.impl.memory.InMemoryProducer /
 * InMemoryStore): every operation on the stock store - store, get, remove, getAll - synchronizes
 * on ONE shared lock, and get(id)/remove(id) scan the whole queue looking for a MessageID match.
 * Under load this becomes the throughput ceiling. ConcurrentHashMap gives O(1) average
 * get/put/remove with no single shared lock.
 *
 * <p>Also confirmed from source (org.apache.synapse.util.MessageHelper): the clone that <store>
 * hands to the producer gets a BRAND NEW message id (UIDGenerator.generateURNString()), not the
 * original request's id - so unlike InMemoryStore, this store does NOT key by
 * MessageContext.getMessageID(). Instead it reads an explicit correlation id from a message
 * context PROPERTY (which survives the clone), configured via the "correlationProperty"
 * parameter (default "CORRELATION_ID").
 *
 * <p><b>Ordering requirement:</b> the deploying sequence must run {@code <store>} BEFORE calling
 * the backend, not after. Confirmed from {@code MessageStoreMediator.mediate()} source:
 * {@code RESPONSE_WRITTEN=SKIP} is set on the parking message's own OperationContext (not just the
 * clone handed to this store), so it is already in effect for a subsequent {@code <call>} on that
 * same context. Storing first means the correlation id exists in this map before the backend has
 * even received the call that would trigger its callback - a request cannot receive a callback for
 * a request it hasn't been sent yet, so there is no window in which the callback can outrun the
 * park. (An earlier revision of this store additionally handled a callback arriving before its
 * park as a runtime race, recording it and completing the match on arrival. Once parking was
 * confirmed to always precede the backend call, that case became unreachable in practice - end to
 * end load testing at 500 concurrent requests and a 10ms backend delay never observed it - so it
 * was removed rather than kept as unexercised complexity. If a future change ever calls the
 * backend before {@code <store>} runs, restore that handling or - simpler - just fix the ordering.)
 *
 * <p>Expiry is a single, store-level setting ("expiryMillis") applied uniformly to every parked
 * request - not something defined per-message. The store sweeps for and fault-responds expired
 * entries entirely internally, on its own background thread (interval controlled by
 * "sweepIntervalMillis"), started in init() and stopped in destroy() - this does not depend on an
 * external {@code <task>} actually being scheduled/running.
 *
 * <p>Two independent layers guarantee the sweep keeps running no matter what:
 * <ol>
 *   <li>sweepAndRespond() itself never lets a Throwable escape (a ScheduledExecutorService
 *       silently and permanently suppresses all future executions of a periodic task the first
 *       time its Runnable throws - confirmed this is the same reason WSO2's own
 *       org.apache.synapse.core.axis2.TimeoutHandler.run() wraps its body in
 *       catch(Exception)/catch(Error)). This also plainly catches OutOfMemoryError: for a
 *       safety-valve task whose entire purpose is preventing an OOM from unreclaimed entries,
 *       swallowing an OOM it happens to hit and trying again next tick is strictly better than
 *       letting that exception permanently disable the one thing that reclaims memory.</li>
 *   <li>An independent watchdog thread (a plain sleeping loop, NOT sharing the sweeper's executor -
 *       if the executor itself somehow died despite (1), a watchdog living on it would die too)
 *       periodically checks whether the sweeper's ScheduledExecutorService is still alive and
 *       recreates it if not. This covers failure modes outside the task body itself.</li>
 * </ol>
 *
 * Example deployment XML:
 * <pre>{@code
 *   <messageStore name="PENDING_REQUESTS" class="com.wso2cre.asyncdemo.ExpiringConcurrentMapMessageStore"
 *                 xmlns="http://ws.apache.org/ns/synapse">
 *       <parameter name="correlationProperty">CORRELATION_ID</parameter>
 *       <parameter name="expiryMillis">25000</parameter>
 *       <parameter name="sweepIntervalMillis">5000</parameter>
 *   </messageStore>
 * }</pre>
 */
public class ExpiringConcurrentMapMessageStore extends AbstractMessageStore {

    private static final Log log = LogFactory.getLog(ExpiringConcurrentMapMessageStore.class.getName());

    public static final String PARAM_CORRELATION_PROPERTY = "correlationProperty";
    public static final String PARAM_EXPIRY_MILLIS = "expiryMillis";
    public static final String PARAM_SWEEP_INTERVAL_MILLIS = "sweepIntervalMillis";

    private static final String DEFAULT_CORRELATION_PROPERTY = "CORRELATION_ID";
    private static final long DEFAULT_EXPIRY_MILLIS = 30000L;
    private static final long DEFAULT_SWEEP_INTERVAL_MILLIS = 5000L;

    private final Map<String, StoredEntry> map = new ConcurrentHashMap<>();

    private String correlationProperty = DEFAULT_CORRELATION_PROPERTY;
    private long expiryMillis = DEFAULT_EXPIRY_MILLIS;
    private long sweepIntervalMillis = DEFAULT_SWEEP_INTERVAL_MILLIS;

    private ScheduledExecutorService sweeper;
    private Thread watchdog;
    private volatile boolean shuttingDown = false;
    private final AtomicBoolean sweepRunning = new AtomicBoolean(false);

    private static final class StoredEntry {
        final MessageContext context;
        final long storedAtMillis;
        final long expiresAtMillis;

        StoredEntry(MessageContext context, long storedAtMillis, long expiresAtMillis) {
            this.context = context;
            this.storedAtMillis = storedAtMillis;
            this.expiresAtMillis = expiresAtMillis;
        }

        boolean isExpired(long now) {
            return now >= expiresAtMillis;
        }
    }

    @Override
    public void init(SynapseEnvironment se) {
        super.init(se);

        Map<String, Object> params = getParameters();
        if (params != null) {
            Object cp = params.get(PARAM_CORRELATION_PROPERTY);
            if (cp != null) {
                correlationProperty = cp.toString();
            }
            Object em = params.get(PARAM_EXPIRY_MILLIS);
            if (em != null) {
                try {
                    expiryMillis = Long.parseLong(em.toString());
                } catch (NumberFormatException nfe) {
                    log.warn("ExpiringConcurrentMapMessageStore [" + getName() + "]: invalid "
                            + PARAM_EXPIRY_MILLIS + " value [" + em + "], using default "
                            + DEFAULT_EXPIRY_MILLIS + "ms.");
                }
            }
            Object sim = params.get(PARAM_SWEEP_INTERVAL_MILLIS);
            if (sim != null) {
                try {
                    sweepIntervalMillis = Long.parseLong(sim.toString());
                } catch (NumberFormatException nfe) {
                    log.warn("ExpiringConcurrentMapMessageStore [" + getName() + "]: invalid "
                            + PARAM_SWEEP_INTERVAL_MILLIS + " value [" + sim + "], using default "
                            + DEFAULT_SWEEP_INTERVAL_MILLIS + "ms.");
                }
            }
        }

        startSweeper();

        log.info("Initialized ExpiringConcurrentMapMessageStore [" + getName() + "] - correlationProperty="
                + correlationProperty + ", expiryMillis=" + expiryMillis + ", sweepIntervalMillis="
                + sweepIntervalMillis);
    }

    @Override
    public void destroy() {
        shuttingDown = true;
        if (sweeper != null) {
            sweeper.shutdownNow();
            sweeper = null;
        }
        if (watchdog != null) {
            watchdog.interrupt();
            watchdog = null;
        }
        super.destroy();
    }

    /**
     * Called once from init(). Starts the sweep executor, then starts the watchdog that keeps it
     * alive - the watchdog itself is only ever started here, never re-started by itself, so
     * destroy() always has exactly one watchdog thread to stop.
     */
    private void startSweeper() {
        shuttingDown = false;
        startSweepExecutor();

        final String storeName = getName();
        watchdog = new Thread(() -> runWatchdog(), "ExpiringConcurrentMapMessageStore-" + storeName + "-watchdog");
        watchdog.setDaemon(true);
        watchdog.start();
    }

    /**
     * Creates (or recreates) just the ScheduledExecutorService + its periodic sweep task. Called
     * from startSweeper() on normal init, and again from the watchdog if it ever finds the
     * executor dead - kept separate from startSweeper() so the watchdog never spawns a second
     * watchdog of its own.
     */
    private void startSweepExecutor() {
        if (sweeper != null) {
            sweeper.shutdownNow();
        }
        final String storeName = getName();
        ThreadFactory threadFactory = r -> {
            Thread t = new Thread(r, "ExpiringConcurrentMapMessageStore-" + storeName + "-sweeper");
            t.setDaemon(true);
            return t;
        };
        sweeper = Executors.newSingleThreadScheduledExecutor(threadFactory);
        sweeper.scheduleWithFixedDelay(this::sweepAndRespond, sweepIntervalMillis, sweepIntervalMillis,
                TimeUnit.MILLISECONDS);
    }

    /**
     * Independent of the sweeper's own executor on purpose: if the executor itself somehow died
     * (not just a single tick throwing - layer 1 below already prevents that from mattering -
     * but e.g. something external calling shutdown() on it, or any other failure mode outside the
     * task body), a watchdog living on that SAME executor would die right along with it and never
     * notice. This is a plain sleeping loop on its own daemon thread, checked and recreated
     * defensively enough that it cannot itself be the thing that stops running.
     */
    private void runWatchdog() {
        while (!shuttingDown) {
            try {
                Thread.sleep(sweepIntervalMillis);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                continue; // loop condition re-checks shuttingDown immediately
            }
            try {
                if (!shuttingDown && (sweeper == null || sweeper.isShutdown() || sweeper.isTerminated())) {
                    log.error("ExpiringConcurrentMapMessageStore [" + getName()
                            + "]: sweep executor was found dead - restarting it now so expiry keeps running.");
                    startSweepExecutor();
                }
            } catch (Throwable t) {
                log.error("ExpiringConcurrentMapMessageStore [" + getName() + "]: watchdog check failed", t);
            }
        }
    }

    /**
     * The sweep body. Must never let an exception escape - a ScheduledExecutorService silently
     * stops rescheduling a periodic task forever after its Runnable throws once, which would
     * turn a single bad entry into a permanent loss of the expiry safety valve. Catches Throwable
     * (not just Exception) deliberately - this includes OutOfMemoryError, and for a safety valve
     * whose entire job is preventing OOM from unreclaimed entries, surviving one and retrying next
     * tick is strictly better than a single OOM permanently disabling the only thing that reclaims
     * memory.
     *
     * sweepRunning guards against overlapping execution (mirroring the same guard in WSO2's own
     * TimeoutHandler.run()) - scheduleWithFixedDelay on a single-thread executor already prevents
     * true overlap, but this stays correct even if that ever changes (e.g. a future refactor to a
     * multi-thread executor).
     */
    private void sweepAndRespond() {
        if (!sweepRunning.compareAndSet(false, true)) {
            return;
        }
        try {
            List<MessageContext> expired = evictExpired();
            for (MessageContext expiredCtx : expired) {
                try {
                    respondWithTimeout(expiredCtx);
                } catch (Throwable t) {
                    log.error("ExpiringConcurrentMapMessageStore [" + getName()
                            + "]: failed to fault-respond an expired entry", t);
                }
            }
        } catch (Throwable t) {
            log.error("ExpiringConcurrentMapMessageStore [" + getName() + "]: sweep iteration failed", t);
        } finally {
            sweepRunning.set(false);
        }
    }

    private void respondWithTimeout(MessageContext expiredCtx) throws Exception {
        String faultJson = "{\"error\":\"TIMEOUT\",\"message\":\"No callback received within the configured timeout.\"}";

        org.apache.axis2.context.MessageContext axis2Ctx =
                ((Axis2MessageContext) expiredCtx).getAxis2MessageContext();

        InputStream in = new ByteArrayInputStream(faultJson.getBytes("UTF-8"));
        JsonUtil.newJsonPayload(axis2Ctx, in, true, true);
        axis2Ctx.setProperty(org.apache.axis2.Constants.Configuration.MESSAGE_TYPE, "application/json");
        axis2Ctx.setProperty(org.apache.axis2.Constants.Configuration.CONTENT_TYPE, "application/json");
        axis2Ctx.setProperty("HTTP_SC", "504");
        // Clears the 202-forcing flag a 202 backend ack leaves on this context (see
        // PassThroughTransportUtils - httpStatus is forced to SC_ACCEPTED whenever this is true).
        axis2Ctx.setProperty("SC_ACCEPTED", Boolean.FALSE);

        expiredCtx.setTo(null);
        expiredCtx.setResponse(true);
        axis2Ctx.getOperationContext().setProperty(org.apache.axis2.Constants.RESPONSE_WRITTEN, "SKIP");
        Axis2Sender.sendBack(expiredCtx);
    }

    public int getType() {
        return Constants.INMEMORY_MS;
    }

    @Override
    public int size() {
        return map.size();
    }

    public MessageProducer getProducer() {
        Producer producer = new Producer(this);
        producer.setId(nextProducerId());
        return producer;
    }

    public MessageConsumer getConsumer() {
        Consumer consumer = new Consumer(this);
        consumer.setId(nextConsumerId());
        return consumer;
    }

    /**
     * Store a message under the given correlation id, expiring at the given epoch millis.
     * Package-visible entry point used by Producer; also usable directly from a custom mediator
     * if you want to bypass the stock &lt;store&gt; mediator entirely.
     */
    void put(String correlationId, MessageContext context, long storedAtMillis, long expiresAtMillis) {
        map.put(correlationId, new StoredEntry(context, storedAtMillis, expiresAtMillis));
        enqueued();
        notifyMessageAddition(correlationId);
    }

    long getExpiryMillis() {
        return expiryMillis;
    }

    String resolveCorrelationId(MessageContext synCtx) {
        Object corr = synCtx.getProperty(correlationProperty);
        if (corr != null && !corr.toString().isEmpty()) {
            return corr.toString();
        }
        // Falls back to the message's own id only if the caller never set a correlation
        // property - this will NOT match a differently-cloned context's id later, so this
        // fallback exists purely to avoid silently dropping a message, not as a supported
        // correlation strategy.
        log.warn("ExpiringConcurrentMapMessageStore [" + getName() + "]: no [" + correlationProperty
                + "] property set on stored message - falling back to MessageContext.getMessageID(), "
                + "which will NOT match across a <store> clone. Set the property explicitly instead.");
        return synCtx.getMessageID();
    }

    public MessageContext get(String correlationId) {
        StoredEntry entry = map.get(correlationId);
        if (entry == null) {
            return null;
        }
        if (entry.isExpired(System.currentTimeMillis())) {
            map.remove(correlationId, entry);
            return null;
        }
        return entry.context;
    }

    public MessageContext remove(String correlationId) {
        StoredEntry entry = map.remove(correlationId);
        if (entry == null) {
            return null;
        }
        dequeued();
        notifyMessageRemoval(correlationId);
        if (entry.isExpired(System.currentTimeMillis())) {
            return null;
        }
        return entry.context;
    }

    /**
     * Removes and returns every entry whose expiry has already passed. Called internally by the
     * store's own sweep thread (see startSweeper()/sweepAndRespond()) - not driven by an external
     * task. Kept public since it's still a reasonable thing to trigger manually (e.g. from a
     * management/JMX hook, or a test).
     *
     * Logs the actual age (now - store time, not now - expiry) of each evicted batch - this is
     * the concrete way to tell whether entries are expiring "just barely" (age close to
     * expiryMillis - the system is running right at capacity) or wildly overdue (age far beyond
     * it - something is properly stuck, e.g. a worker-pool queue backlog), rather than guessing
     * from the eviction count alone.
     */
    public List<MessageContext> evictExpired() {
        long now = System.currentTimeMillis();
        List<MessageContext> expired = new ArrayList<>();
        long minAge = Long.MAX_VALUE;
        long maxAge = Long.MIN_VALUE;
        long sumAge = 0;
        for (Iterator<Map.Entry<String, StoredEntry>> it = map.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, StoredEntry> e = it.next();
            StoredEntry entry = e.getValue();
            if (entry.isExpired(now)) {
                it.remove();
                dequeued();
                notifyMessageRemoval(e.getKey());
                expired.add(entry.context);
                long age = now - entry.storedAtMillis;
                minAge = Math.min(minAge, age);
                maxAge = Math.max(maxAge, age);
                sumAge += age;
            }
        }
        if (!expired.isEmpty()) {
            log.info("ExpiringConcurrentMapMessageStore [" + getName() + "]: evicted " + expired.size()
                    + " expired entr" + (expired.size() == 1 ? "y" : "ies") + " - age at eviction (ms) min="
                    + minAge + " max=" + maxAge + " avg=" + (sumAge / expired.size())
                    + " (configured expiryMillis=" + expiryMillis + ") - age close to " + expiryMillis
                    + " means the system is running right at capacity; age far beyond it means something is "
                    + "properly stuck (e.g. worker-pool queue backlog), not just marginal overload.");
        }
        return expired;
    }

    public void clear() {
        map.clear();
    }

    public MessageContext remove() throws NoSuchElementException {
        Iterator<Map.Entry<String, StoredEntry>> it = map.entrySet().iterator();
        if (!it.hasNext()) {
            throw new NoSuchElementException("ExpiringConcurrentMapMessageStore [" + getName() + "] is empty.");
        }
        Map.Entry<String, StoredEntry> e = it.next();
        it.remove();
        dequeued();
        notifyMessageRemoval(e.getKey());
        return e.getValue().context;
    }

    public MessageContext get(int index) {
        List<StoredEntry> snapshot = new ArrayList<>(map.values());
        if (index < 0 || index >= snapshot.size()) {
            return null;
        }
        return snapshot.get(index).context;
    }

    public List<MessageContext> getAll() {
        List<MessageContext> result = new ArrayList<>();
        for (StoredEntry e : map.values()) {
            result.add(e.context);
        }
        return result;
    }

    private static final class Producer implements MessageProducer {
        private final ExpiringConcurrentMapMessageStore store;
        private String idString;

        Producer(ExpiringConcurrentMapMessageStore store) {
            this.store = store;
        }

        public boolean storeMessage(MessageContext synCtx) {
            if (synCtx == null) {
                return false;
            }
            String correlationId = store.resolveCorrelationId(synCtx);
            long now = System.currentTimeMillis();
            long expiresAt = now + store.getExpiryMillis();
            store.put(correlationId, synCtx, now, expiresAt);
            if (log.isDebugEnabled()) {
                log.debug(getId() + " stored correlation id [" + correlationId + "], expires in "
                        + (expiresAt - now) + "ms, store size now " + store.size());
            }
            return true;
        }

        public boolean cleanup() {
            return true;
        }

        public void setId(int id) {
            idString = "[" + store.getName() + "-P-" + id + "]";
        }

        public String getId() {
            return idString;
        }
    }

    private static final class Consumer implements MessageConsumer {
        private final ExpiringConcurrentMapMessageStore store;
        private String idString;
        private boolean alive = true;

        Consumer(ExpiringConcurrentMapMessageStore store) {
            this.store = store;
        }

        /**
         * This store is designed to be drained by explicit correlation id (see
         * CompleteRequestMediator) or by the store's own internal sweep, not by FIFO consumption
         * through a Message Processor - there is no meaningful "next" message for an id-keyed
         * map. Always returns null.
         */
        public MessageContext receive() {
            return null;
        }

        public boolean ack() {
            return true;
        }

        public boolean cleanup() {
            return true;
        }

        public boolean isAlive() {
            return alive;
        }

        public void setAlive(boolean isAlive) {
            this.alive = isAlive;
        }

        public void setId(int id) {
            idString = "[" + store.getName() + "-C-" + id + "]";
        }

        public String getId() {
            return idString;
        }

        public boolean reInitialize() {
            // No connection/session state to reset for this store.
            return true;
        }
    }
}
