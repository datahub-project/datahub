import { IdGenerator, RandomIdGenerator } from '@opentelemetry/sdk-trace-web';

/**
 * Browser port of the backend `TraceIdGenerator`
 * (metadata-operation-context/.../context/TraceIdGenerator.java).
 *
 * A DataHub trace ID is a 32-char hex string in two 8-byte halves:
 *   - first 16 hex chars  → timestamp in MICROS (epochMillis * 1000)
 *   - last 16 hex chars   → random, for uniqueness
 *
 * Encoding wall-clock time into the ID lets the backend derive it straight from the ID
 * (`TraceIdGenerator.getTimestampMillis`) — driving Kafka queue-time metrics and async write-status
 * classification with no extra metadata. A random W3C trace ID carries no such time, so
 * browser-initiated traces miss all of that; matching the backend format restores it.
 *
 * INTEGER SAFETY: the timestamp half (epochMicros) already exceeds the range JS can format from a
 * double, and a 64-bit random half overflows a double entirely. All 64-bit values are handled with
 * BigInt / raw bytes; Number is never used for the ID, so there is no silent precision loss.
 */
const TRACE_ID_RANDOM_BYTES = 8;

// DataHub's tracing system didn't exist before 2020.
const MIN_PLAUSIBLE_EPOCH_MS = 1577836800000; // 2020-01-01 UTC
const MAX_CLOCK_SKEW_MS = 86400000; // 24 hours

// BigInt(1000) instead of a 1000n literal — tsconfig targets es2017, which forbids BigInt
// literal syntax (TS2737). esnext lib provides the BigInt type itself.
const MICROS_PER_MILLIS = BigInt(1000);

export class OtelTraceIdGenerator implements IdGenerator {
    private readonly spanIdGenerator: IdGenerator;

    // Mirrors Java's `defaultGenerator = IdGenerator.random()` — span IDs use the SDK's vetted
    // random generator (16 hex chars, all-zero retry handled internally).
    constructor(spanIdGenerator: IdGenerator = new RandomIdGenerator()) {
        this.spanIdGenerator = spanIdGenerator;
    }

    generateTraceId(): string {
        return this.traceIdForEpochMillis(Date.now());
    }

    generateSpanId(): string {
        return this.spanIdGenerator.generateSpanId();
    }

    /** Exposed for testing — deterministic timestamp half, random low half. */
    traceIdForEpochMillis(epochMillis: number): string {
        // Match Java's `long timestampMicros = epochMillis * 1000` exactly via BigInt.
        const timestampMicros = BigInt(epochMillis) * MICROS_PER_MILLIS;
        const timestampHex = timestampMicros.toString(16).padStart(16, '0');
        // No trace-id all-zero retry: the timestamp half is never zero post-2020, so the full
        // 32-char ID can never be all-zero (unlike the SDK's RandomIdGenerator, which must retry).
        return `${timestampHex}${randomHex(TRACE_ID_RANDOM_BYTES)}`;
    }

    /**
     * Extracts the epoch millis encoded in a DataHub trace ID. Returns null if the trace ID is
     * malformed, too short, or encodes an implausible timestamp (e.g., external W3C trace IDs from
     * OTel service meshes where the first 16 hex chars are random, not epoch micros).
     *
     * Port of `TraceIdGenerator.getTimestampMillis`. Bounds checked in BigInt to preserve Java's
     * unsigned-64-bit semantics; the final `Number()` only runs after the value passes the window
     * (a passing value is ≤ now+24h ~1.7e12 < 2^53, so the conversion is exact).
     */
    static getTimestampMillis(traceId: string | null): number | null {
        if (traceId === null || traceId.length < 16) {
            return null;
        }
        const first16 = traceId.substring(0, 16);
        if (!/^[0-9a-fA-F]{16}$/.test(first16)) {
            return null;
        }
        const epochMillisBig = BigInt(`0x${first16}`) / MICROS_PER_MILLIS;
        const nowPlusSkew = BigInt(Date.now() + MAX_CLOCK_SKEW_MS);
        if (epochMillisBig >= BigInt(MIN_PLAUSIBLE_EPOCH_MS) && epochMillisBig <= nowPlusSkew) {
            return Number(epochMillisBig);
        }
        return null;
    }
}

/** `numBytes` cryptographically-random bytes as lowercase hex (2 chars per byte). */
function randomHex(numBytes: number): string {
    const bytes = new Uint8Array(numBytes);
    crypto.getRandomValues(bytes);
    return Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('');
}
