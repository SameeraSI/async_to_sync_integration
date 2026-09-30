# Async Callback Demo — MI + Self-Contained Custom Message Store

A reference implementation for a common integration pattern: expose a REST API in WSO2 Micro
Integrator that calls a backend, gets a quick ack, parks the client's connection (no worker thread
held while waiting), and completes the response later from a separate callback resource once the
backend's real result arrives — all without blocking a thread for the wait.

## The problem

Some backends answer in two steps: you call them, they say "got it" right away, and the real
result only arrives later, on a separate callback call. Exposing that as one normal-looking
synchronous REST call — without a worker thread sitting idle for however long the backend takes —
needs a way to pause the client's connection and complete it later, when the real result shows up.

A naive fix (poll a cache/DB in a loop with `Thread.sleep`) blocks a worker thread for the entire
wait. This project uses MI's own message-store mechanism instead, so **zero threads are blocked**
regardless of how long the backend takes.

## How it works

1. **Tag the request with a correlation id**, so the callback can later be matched back to it.

   ```xml
   <property expression="get-property('MessageID')" name="CORRELATION_ID" scope="default" type="STRING"/>
   <header name="X-Correlation-Id" scope="transport" expression="$ctx:CORRELATION_ID"/>
   ```

2. **Hand the request to MI's built-in `<store>` mediator — before calling the backend, not
   after.** This one line does two things automatically: it clones the request while keeping the
   client's live connection attached, and tells MI not to send a response yet.

   ```xml
   <store messageStore="PENDING_REQUESTS"/>
   ```

   Parking *before* the backend call is deliberate: the backend starts its own callback timer the
   instant it receives the request, independent of how long MI then takes to run the rest of the
   sequence. If the call ran first, a fast callback under load could reach the retrieval side
   before this request had even parked. Storing first means the correlation id exists before the
   backend has even received the call that could trigger its callback — a hard ordering guarantee,
   not a smaller race window.

3. **Only now call the backend's quick-ack endpoint.** This call returns almost immediately and is
   not the real result — nothing from it is used afterward.

   ```xml
   <call>
       <endpoint key="BackendAckEndpoint"/>
   </call>
   ```

   The sequence ends right there. The worker thread goes straight back into the pool, free to serve
   other requests. The client's connection stays open, but no thread is spent holding it.

4. **When the callback arrives** (a separate request, on a different thread), the same correlation
   id is used to pull the exact matching parked request out of the store, attach the real result to
   it, and send it back on the original connection.

   ```java
   MessageStore store = synCtx.getConfiguration().getMessageStore(storeName);
   MessageContext parkedCtx = store.remove(correlationId);
   // ... attach the real result to parkedCtx ...
   Axis2Sender.sendBack(parkedCtx);
   ```

5. **If the callback never comes, the store times it out on its own.** Every parked request has one
   expiry, and a background sweep inside the store (running every few seconds) finds anything that
   waited too long and sends a `504 TIMEOUT` — so nothing hangs forever and nothing leaks memory.

## Why a custom message store, not the stock `InMemoryStore`

MI ships with a built-in in-memory store, and it was tried first. It works correctly but doesn't
scale, for two reasons:

- **It can't search by an application-defined correlation id.** `InMemoryStore.get(id)`/`remove(id)`
  only match Synapse's own internal message id — and the clone `<store>` creates is assigned a
  brand-new internal id anyway. The only way to find "the one request with correlation id X" was to
  pull back the entire list of parked requests (`getAll()`) and scan it by hand, every single time
  a callback arrived.
- **It doesn't scale under concurrency.** Under the hood, `InMemoryStore` backs its queue with a
  `ConcurrentLinkedQueue` — already lock-free on its own — but every operation, including plain
  `storeMessage()`, is additionally wrapped in one shared `synchronized` lock, negating that
  structure's own concurrency; all producers and consumers fully serialize on a single lock.
  Separately, a linked list has no random-access or keyed index, so any id-based lookup or a
  `getAll()` scan is an O(n) traversal by construction. Together these mean throughput degrades as
  the number of concurrently parked requests grows — contention was measured starting around
  **~700 requests/second** in testing.

The fix: `ExpiringConcurrentMapMessageStore` (`mediator/src/main/java/com/wso2cre/asyncdemo/`) keeps
parked requests in a `ConcurrentHashMap` keyed directly by the correlation id — O(1) store, lookup,
and remove regardless of how many other requests are parked, no manual searching, no single shared
lock. It deploys and is used **exactly like the stock in-memory store**:

```xml
<messageStore name="PENDING_REQUESTS" class="com.wso2cre.asyncdemo.ExpiringConcurrentMapMessageStore">
    <parameter name="correlationProperty">CORRELATION_ID</parameter>
    <parameter name="expiryMillis">25000</parameter>
    <parameter name="sweepIntervalMillis">5000</parameter>
</messageStore>
```

- `correlationProperty` — the message-context property used as the lookup key (default
  `CORRELATION_ID`), since `<store>`'s clone gets a brand-new internal message id and can't be
  looked up by it.
- `expiryMillis` — a single expiry applied to every parked request (not settable per-message).
- `sweepIntervalMillis` — how often the store's own internal background thread checks for and
  fault-responds expired entries.

Nothing extra needs to be deployed or scheduled separately for expiry to work: the sweep is a
daemon thread the store starts in `init()` and stops in `destroy()`, with two independent layers
guaranteeing it can't silently stop (a `try/catch(Throwable)` around the sweep body — a
`ScheduledExecutorService` otherwise permanently cancels all future runs the first time its
`Runnable` throws once — plus an independent watchdog thread that restarts the executor if it's
ever found dead).

Only `CompleteRequestMediator` (retrieval on the callback path) needs custom code. It uses the
standard `MessageStore.remove(String)` interface method, so it works against any `MessageStore`
implementation, not just this one.

Validated end-to-end under load: **over 1,500 requests/second with zero blocked worker threads**,
versus roughly 700 requests/second before hitting contention with the stock `InMemoryStore`. Also
load-tested at a deliberately short backend delay (10ms) and 500 concurrent requests to confirm the
store-before-call ordering above fully eliminates the park-vs-callback race — zero warnings, zero
failures.

## Project layout

```
async_to_sync_integration/                 the WSO2 MI integration project (open this folder in
                                            the WSO2 Micro Integrator VS Code extension) - Synapse
                                            artifacts + Maven/CAR/Docker packaging only
  src/main/wso2mi/artifacts/
    apis/AsyncCallbackDemoAPI.xml            POST /asyncdemo/orders , POST /asyncdemo/callback
    sequences/ParkAndCallSequence.xml        calls backend, parks the request (stock <store> mediator)
    sequences/CompleteRequestSequence.xml    callback handler - retrieves + completes the parked request
    message-stores/PENDING_REQUESTS.xml      the custom message store (class + parameters)
    endpoints/BackendAckEndpoint.xml         the backend's quick-ack endpoint

mediator/                                  standalone source + build script for the custom message
                                            store - deliberately separate from the MI project above:
                                            the store's class is loaded via Class.forName(), which
                                            only ever checks MI's own lib/ folder, not anything
                                            bundled inside the CAR - so there's no benefit to
                                            compiling it as part of the MI project itself.
  build.sh                                 compiles + jars the two classes below against a local
                                            MI_HOME (see "Building" below)
  src/main/java/com/wso2cre/asyncdemo/
    ExpiringConcurrentMapMessageStore.java   the custom MessageStore implementation
    CompleteRequestMediator.java             retrieves + completes a parked request on callback

mock-backend/mock-backend.js               zero-dependency Node.js mock of an async backend,
                                            for local testing without a real one
```

## Building

**The mediator** (requires a local WSO2 Micro Integrator 4.4.0 installation, to compile against the
exact Synapse/Axis2 classes MI 4.4.0 loads at runtime):

```bash
export MI_HOME=/path/to/wso2mi-4.4.0
./mediator/build.sh
```

This produces `mediator/target/async-to-sync-integration-mediator-1.0.0.jar`.

The build deliberately puts every jar under `$MI_HOME/wso2/components/plugins/` and
`$MI_HOME/wso2/lib/` on the compile classpath rather than depending on the exact filename of any
one of them - those filenames embed a WSO2 update-level patch suffix (e.g.
`synapse-core_4.0.0.wso2v215_20.jar` vs `..._7.jar`) that varies across installations, so a
`javac`/wildcard-classpath approach here is more robust than a Maven `systemPath` dependency, which
must resolve to one exact, hardcoded filename.

**The MI project** (only needs Java 8+/Maven - no MI installation required, since it contains no
Java source of its own):

```bash
cd async_to_sync_integration
mvn clean install
```

This produces `async_to_sync_integration/target/async_to_sync_integration_1.0.0.car`.

## Deploying

The custom message store's class is loaded by Synapse via `Class.forName()`, which only ever
checks MI's own `lib/` folder - not the CAR's contents. Deploy the CAR and the mediator jar
separately:

```bash
cp async_to_sync_integration/target/async_to_sync_integration_1.0.0.car \
  $MI_HOME/repository/deployment/server/carbonapps/
cp mediator/target/async-to-sync-integration-mediator-1.0.0.jar $MI_HOME/lib/
```

Then start MI as usual.

### Docker

```bash
cd async_to_sync_integration
mkdir -p deployment/libs
cp ../mediator/target/async-to-sync-integration-mediator-1.0.0.jar deployment/libs/
```

Then uncomment the `COPY libs/*.jar ...` line in `deployment/docker/Dockerfile` (left commented out
by default so the image builds cleanly even without the mediator jar present), and:

```bash
mvn -P docker clean install
docker run -p 8290:8290 async_to_sync_integration:1.0.0
```

## Testing locally

Start the included mock backend (defaults to port 9091, calling back to
`http://localhost:8290/asyncdemo/callback`):

```bash
node mock-backend/mock-backend.js
```

Then call the API:

```bash
# Happy path - backend replies after 2 seconds
curl -X POST http://localhost:8290/asyncdemo/orders \
  -H "Content-Type: application/json" \
  -d '{"result":"order-processed","delayMs":2000}'

# Never-callback - times out with a 504 after expiryMillis
curl -X POST http://localhost:8290/asyncdemo/orders \
  -H "Content-Type: application/json" \
  -d '{"neverCallback":true}'
```

Set `WORKERS=N` to scale the mock backend across CPU cores and `VERBOSE=1` for per-request logging
when load-testing (see comments at the top of `mock-backend.js` for details).
