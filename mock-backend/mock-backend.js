// Zero-dependency mock backend for load-testing the async-callback-demo MI project.
// Node.js built-ins only (http, cluster, os) - no npm install required.
//
// Scaled for high throughput (target: 5000+ TPS):
//   - cluster module forks one worker per CPU core, all sharing the same listening port -
//     a single Node process is single-threaded and was the actual bottleneck at load, not MI.
//   - a persistent keep-alive http.Agent is reused for every outbound callback call, instead of
//     opening a fresh TCP connection per callback (which is what kills throughput under load).
//   - per-request console.log is OFF by default (synchronous stdout writes become the real
//     bottleneck once you're past a few hundred req/sec) - only the master process prints
//     aggregated throughput stats once a second. Set VERBOSE=1 to get old-style per-request logs
//     back for low-volume debugging.
//
// POST /backend/submit
//   Body (JSON): { "result": "...", "delayMs": 2000, "neverCallback": false }
//   Responds immediately with 202 {"ack":true}, then - unless neverCallback is true - waits
//   delayMs milliseconds and POSTs the result back to MI's callback resource, echoing the
//   X-Correlation-Id header MI sent on the original call.
//
// Usage: node mock-backend.js [port] [miCallbackUrl]
//   node mock-backend.js                     -> listens on 9091, calls back http://localhost:8285/asyncdemo/callback
//   node mock-backend.js 9091 http://localhost:8285/asyncdemo/callback
//
// Env vars:
//   WORKERS=8      number of worker processes (default: number of CPU cores)
//   VERBOSE=1      log every submit/callback individually instead of just aggregate stats

const http = require('http');
const cluster = require('cluster');
const os = require('os');
const { URL } = require('url');

const PORT = process.argv[2] ? parseInt(process.argv[2], 10) : 9091;
const CALLBACK_URL = process.argv[3] || 'http://localhost:8285/asyncdemo/callback';
const WORKERS = process.env.WORKERS ? parseInt(process.env.WORKERS, 10) : os.cpus().length;
const VERBOSE = process.env.VERBOSE === '1';

if (cluster.isMaster || cluster.isPrimary) {
    console.log(`Mock backend (master pid ${process.pid}) starting ${WORKERS} worker(s) on port ${PORT}`);
    console.log(`Will call back to: ${CALLBACK_URL}`);

    const EMPTY_STATS = { submits: 0, callbacksSent: 0, callbackFailures: 0, driftCount: 0, driftSumMs: 0, driftMaxMs: 0 };
    const workerStats = new Map();
    for (let i = 0; i < WORKERS; i++) {
        const w = cluster.fork();
        workerStats.set(w.id, { ...EMPTY_STATS });
        w.on('message', (msg) => {
            if (msg && msg.type === 'stats') {
                workerStats.set(w.id, msg.data);
            }
        });
    }

    // Sliding-window crash-loop guard (a fixed-interval reset here would let a persistent
    // failure - e.g. the port already being in use - retry forever at a slow trickle instead
    // of actually stopping).
    const EXIT_WINDOW_MS = 10000;
    const MAX_EXITS_IN_WINDOW = WORKERS * 3;
    let exitTimestamps = [];
    cluster.on('exit', (worker, code, signal) => {
        workerStats.delete(worker.id);
        const now = Date.now();
        exitTimestamps.push(now);
        exitTimestamps = exitTimestamps.filter((t) => now - t <= EXIT_WINDOW_MS);
        if (exitTimestamps.length > MAX_EXITS_IN_WINDOW) {
            console.error(`Worker ${worker.process.pid} died (${signal || code}) - too many worker deaths within ${EXIT_WINDOW_MS}ms (likely the port is already in use), giving up.`);
            process.exit(1);
        }
        console.error(`Worker ${worker.process.pid} died (${signal || code}), restarting`);
        const nw = cluster.fork();
        workerStats.set(nw.id, { ...EMPTY_STATS });
    });

    let lastTotals = { ...EMPTY_STATS };
    setInterval(() => {
        const totals = { ...EMPTY_STATS };
        for (const s of workerStats.values()) {
            totals.submits += s.submits;
            totals.callbacksSent += s.callbacksSent;
            totals.callbackFailures += s.callbackFailures;
            totals.driftCount += s.driftCount;
            totals.driftSumMs += s.driftSumMs;
            totals.driftMaxMs = Math.max(totals.driftMaxMs, s.driftMaxMs);
        }
        const submitRate = totals.submits - lastTotals.submits;
        const callbackRate = totals.callbacksSent - lastTotals.callbacksSent;
        const intervalDriftCount = totals.driftCount - lastTotals.driftCount;
        const intervalDriftSum = totals.driftSumMs - lastTotals.driftSumMs;
        const avgDrift = intervalDriftCount > 0 ? Math.round(intervalDriftSum / intervalDriftCount) : 0;
        // "drift" = how much LATER than the requested delayMs the callback actually fired -
        // i.e. setTimeout(fn, delayMs) firing late. A non-trivial, growing avgDrift here means
        // THIS backend's own event loop is saturated under load and is itself adding delay on
        // top of whatever delayMs was requested - a real, well-known Node.js symptom, and
        // directly implicates the backend side rather than MI.
        console.log(`[stats] submits=${totals.submits} (+${submitRate}/s) callbacksSent=${totals.callbacksSent} (+${callbackRate}/s) callbackFailures=${totals.callbackFailures} timerDriftMs(avg/max this interval)=${avgDrift}/${totals.driftMaxMs}`);
        lastTotals = totals;
    }, 1000);

} else {
    // Reused across every outbound callback call - this is what avoids a fresh TCP connection
    // (and the connection-setup overhead / ephemeral port churn that comes with it) per callback.
    const keepAliveAgent = new http.Agent({ keepAlive: true, maxSockets: 2048, maxFreeSockets: 256 });

    let submits = 0;
    let callbacksSent = 0;
    let callbackFailures = 0;
    // Cumulative (master derives per-interval rate by subtracting the previous total, same as
    // submits/callbacksSent) - but driftMaxMs is reset after each report, since "max ever" isn't
    // useful for spotting a CURRENT degradation, only "max in the last second" is.
    let driftCount = 0;
    let driftSumMs = 0;
    let driftMaxMs = 0;

    function reportStats() {
        if (process.send) {
            process.send({ type: 'stats', data: { submits, callbacksSent, callbackFailures, driftCount, driftSumMs, driftMaxMs } });
        }
        driftMaxMs = 0;
    }
    setInterval(reportStats, 1000);

    function readBody(req) {
        return new Promise((resolve) => {
            let raw = '';
            req.on('data', (chunk) => { raw += chunk; });
            req.on('end', () => {
                try {
                    resolve(raw ? JSON.parse(raw) : {});
                } catch (e) {
                    resolve({});
                }
            });
        });
    }

    function postCallback(correlationId, payload) {
        const url = new URL(CALLBACK_URL);
        const body = JSON.stringify(payload);
        const options = {
            hostname: url.hostname,
            port: url.port,
            path: url.pathname,
            method: 'POST',
            agent: keepAliveAgent,
            headers: {
                'Content-Type': 'application/json',
                'Content-Length': Buffer.byteLength(body),
                'X-Correlation-Id': correlationId
            }
        };
        const req = http.request(options, (res) => {
            res.resume(); // drain the response body, don't bother buffering it
            res.on('end', () => {
                callbacksSent++;
                if (VERBOSE) {
                    console.log(`[callback] -> correlationId=${correlationId} status=${res.statusCode}`);
                }
            });
        });
        req.on('error', (e) => {
            callbackFailures++;
            if (VERBOSE) {
                console.error(`[callback] FAILED for correlationId=${correlationId}:`, e.message);
            }
        });
        req.write(body);
        req.end();
    }

    const server = http.createServer(async (req, res) => {
        if (req.method === 'POST' && req.url === '/backend/submit') {
            const correlationId = req.headers['x-correlation-id'] || 'UNKNOWN';
            const body = await readBody(req);
            const delayMs = typeof body.delayMs === 'number' ? body.delayMs : 2000;
            const neverCallback = body.neverCallback === true;
            const result = body.result || `processed-${correlationId}`;

            submits++;
            if (VERBOSE) {
                console.log(`[submit] correlationId=${correlationId} delayMs=${delayMs} neverCallback=${neverCallback}`);
            }

            res.writeHead(202, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ ack: true, correlationId, message: 'processing' }));

            if (!neverCallback) {
                const scheduledFireTime = Date.now() + delayMs;
                setTimeout(() => {
                    const actualFireTime = Date.now();
                    const drift = actualFireTime - scheduledFireTime;
                    if (drift > 0) {
                        driftCount++;
                        driftSumMs += drift;
                        driftMaxMs = Math.max(driftMaxMs, drift);
                    }
                    postCallback(correlationId, { correlationId, result });
                }, delayMs);
            }
            return;
        }

        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'not found' }));
    });

    // Must exceed MI's own idle-connection timeout, or this end could close a reusable
    // keep-alive connection out from under a client that's about to reuse it under load.
    server.keepAliveTimeout = 65000;
    server.headersTimeout = 66000;

    server.listen(PORT);
}
