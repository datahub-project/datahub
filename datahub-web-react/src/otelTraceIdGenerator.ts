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
 * INTEGER SAFETY: the 64-bit random half (up to 2^64 ≈ 1.8e19) overflows a JS double entirely
 * (Number.MAX_SAFE_INTEGER ≈ 9e15), so it is built from raw crypto bytes, never Number. The
 * timestamp half (epochMicros ≈ 1.7e15 today) sits under 2^53 and wouldn't overflow until ~year
 * 2255, but BigInt is used there too for uniformity — the whole ID path stays BigInt / raw bytes,
 * so Number is never used for the ID and there is no silent precision loss.
 */
const TRACE_ID_RANDOM_BYTES = 8;

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
}

/** `numBytes` cryptographically-random bytes as lowercase hex (2 chars per byte). */
function randomHex(numBytes: number): string {
    const bytes = new Uint8Array(numBytes);
    crypto.getRandomValues(bytes);
    return Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('');
}
