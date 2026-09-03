import { describe, expect, it } from 'vitest';

import { buildCodeDiff, shouldRenderCodeBlockDiff } from '@components/components/CodeBlock/diffCode';

describe('shouldRenderCodeBlockDiff', () => {
    it('is off when diffAgainst is omitted or matches code', () => {
        expect(shouldRenderCodeBlockDiff('SELECT 1')).toBe(false);
        expect(shouldRenderCodeBlockDiff('SELECT 1', 'SELECT 1')).toBe(false);
        expect(shouldRenderCodeBlockDiff('SELECT 1\n', 'SELECT 1')).toBe(false);
    });

    it('is on when the strings differ', () => {
        expect(shouldRenderCodeBlockDiff("status = 'active'", "status IN ('active','trial')")).toBe(true);
    });
});

describe('buildCodeDiff', () => {
    it('repeats a changed line with word-level chips', () => {
        const lines = buildCodeDiff(
            "COUNT(DISTINCT CASE WHEN campaigns.status = 'active'\nTHEN campaigns.id END)",
            "COUNT(DISTINCT CASE WHEN campaigns.status IN ('active','trial')\nTHEN campaigns.id END)",
        );

        expect(lines).toHaveLength(3);
        expect(lines[0]?.kind).toBe('removed');
        expect(lines[1]?.kind).toBe('added');
        expect(lines[2]).toEqual({
            kind: 'unchanged',
            segments: [{ type: 'equal', value: 'THEN campaigns.id END)' }],
        });

        const removedText = lines[0]?.segments
            .filter((segment) => segment.type === 'removed')
            .map((segment) => segment.value)
            .join('');
        const addedText = lines[1]?.segments
            .filter((segment) => segment.type === 'added')
            .map((segment) => segment.value)
            .join('');
        expect(removedText).toContain("= 'active'");
        expect(addedText).toContain("IN ('active','trial')");
    });

    it('keeps unchanged lines as a single equal segment', () => {
        const lines = buildCodeDiff('SELECT 1\nFROM t', 'SELECT 1\nFROM t');
        expect(lines).toEqual([
            { kind: 'unchanged', segments: [{ type: 'equal', value: 'SELECT 1' }] },
            { kind: 'unchanged', segments: [{ type: 'equal', value: 'FROM t' }] },
        ]);
    });

    it('treats leftover added lines as whole-line additions', () => {
        const lines = buildCodeDiff('SELECT 1', 'SELECT 1\nQUALIFY rn = 1');
        expect(lines[0]?.kind).toBe('unchanged');
        expect(lines[1]).toEqual({
            kind: 'added',
            segments: [{ type: 'added', value: 'QUALIFY rn = 1' }],
        });
    });
});
