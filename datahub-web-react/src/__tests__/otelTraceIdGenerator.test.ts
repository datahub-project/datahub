import { describe, expect, it } from 'vitest';

import { OtelTraceIdGenerator } from '@src/otelTraceIdGenerator';

const HEX32 = /^[0-9a-f]{32}$/;
const HEX16 = /^[0-9a-f]{16}$/;

// Port of the backend TraceIdGenerator.getTimestampMillis — lives in the test because the browser
// has no runtime caller. The round-trip case below is what proves browser-generated IDs pass the
// backend's plausibility window. Re-extract into a prod module if a UI trace-correlation feature
// ever needs to parse trace IDs client-side.
const MIN_PLAUSIBLE_EPOCH_MS = 1577836800000; // 2020-01-01 UTC
const MAX_CLOCK_SKEW_MS = 86400000; // 24 hours
const MICROS_PER_MILLIS = BigInt(1000);

function getTimestampMillis(traceId: string | null): number | null {
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

describe('OtelTraceIdGenerator', () => {
    describe('generateTraceId', () => {
        it('returns a 32-char lowercase hex string', () => {
            const gen = new OtelTraceIdGenerator();
            expect(HEX32.test(gen.generateTraceId())).toBe(true);
        });

        it('encodes the current time in the high 16 hex chars', () => {
            const gen = new OtelTraceIdGenerator();
            const before = Date.now();
            const id = gen.generateTraceId();
            const after = Date.now();

            const microsBig = BigInt(`0x${id.substring(0, 16)}`);
            // High half is epochMillis * 1000, captured between before/after.
            expect(microsBig).toBeGreaterThanOrEqual(BigInt(before) * BigInt(1000));
            expect(microsBig).toBeLessThanOrEqual(BigInt(after) * BigInt(1000));
        });
    });

    describe('traceIdForEpochMillis', () => {
        it('puts hex(epochMillis * 1000) padded to 16 in the high half', () => {
            const gen = new OtelTraceIdGenerator();
            const epochMillis = 1700000000000; // 2023-11-14
            const id = gen.traceIdForEpochMillis(epochMillis);

            expect(id).toHaveLength(32);
            expect(id.substring(0, 16)).toBe((BigInt(epochMillis) * BigInt(1000)).toString(16).padStart(16, '0'));
            expect(HEX16.test(id.substring(16, 32))).toBe(true);
        });
    });

    describe('getTimestampMillis', () => {
        it('round-trips a generated trace ID back to its epoch millis', () => {
            const gen = new OtelTraceIdGenerator();
            const epochMillis = 1700000000000;
            const id = gen.traceIdForEpochMillis(epochMillis);
            expect(getTimestampMillis(id)).toBe(epochMillis);
        });

        it('returns null for null / short / non-hex inputs', () => {
            expect(getTimestampMillis(null)).toBeNull();
            expect(getTimestampMillis('abc')).toBeNull();
            expect(getTimestampMillis(`gggggggggggggggg${'0'.repeat(16)}`)).toBeNull();
        });

        it('returns null for a pre-2020 timestamp', () => {
            const gen = new OtelTraceIdGenerator();
            const id = gen.traceIdForEpochMillis(1546300800000); // 2019-01-01
            expect(getTimestampMillis(id)).toBeNull();
        });

        it('returns null for a timestamp more than 24h in the future', () => {
            const gen = new OtelTraceIdGenerator();
            const id = gen.traceIdForEpochMillis(Date.now() + 25 * 60 * 60 * 1000);
            expect(getTimestampMillis(id)).toBeNull();
        });
    });

    describe('generateSpanId', () => {
        it('delegates to the injected IdGenerator', () => {
            const stub = { generateSpanId: () => '0123456789abcdef', generateTraceId: () => 'f'.repeat(32) };
            const gen = new OtelTraceIdGenerator(stub);
            expect(gen.generateSpanId()).toBe('0123456789abcdef');
        });

        it('default generator returns a 16-char lowercase hex, never all-zero', () => {
            const gen = new OtelTraceIdGenerator();
            for (let i = 0; i < 100; i += 1) {
                const spanId = gen.generateSpanId();
                expect(HEX16.test(spanId)).toBe(true);
                expect(spanId).not.toBe('0000000000000000');
            }
        });
    });
});
