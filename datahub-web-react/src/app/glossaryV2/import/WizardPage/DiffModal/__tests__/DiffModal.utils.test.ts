import { describe, expect, it } from 'vitest';

import { buildLineDiff, wordSegments } from '@app/glossaryV2/import/WizardPage/DiffModal/DiffModal.utils';

const highlighted = (segments: { text: string; highlighted: boolean }[]) =>
    segments
        .filter((s) => s.highlighted)
        .map((s) => s.text)
        .join('');

describe('wordSegments', () => {
    it('highlights only the words that changed within a line', () => {
        const left = wordSegments('Total income generated from sales', 'Total revenue generated from sales', 'left');
        const right = wordSegments('Total income generated from sales', 'Total revenue generated from sales', 'right');

        // Only the differing word is highlighted; shared words are plain.
        expect(highlighted(left)).toBe('income');
        expect(highlighted(right)).toBe('revenue');
        expect(left.some((s) => s.text.includes('Total') && s.highlighted)).toBe(false);
        expect(left.some((s) => s.text.includes('generated') && s.highlighted)).toBe(false);
    });

    it('omits added tokens from the left side and removed tokens from the right side', () => {
        const left = wordSegments('a b', 'a b c', 'left')
            .map((s) => s.text)
            .join('');
        const right = wordSegments('a b', 'a b c', 'right')
            .map((s) => s.text)
            .join('');
        expect(left).not.toContain('c');
        expect(left.trim()).toBe('a b');
        expect(right.trim()).toBe('a b c');
    });
});

describe('buildLineDiff', () => {
    it('treats identical text as context with no highlights on either side', () => {
        const { left, right } = buildLineDiff('Same text', 'Same text');
        expect(left).toHaveLength(1);
        expect(right).toHaveLength(1);
        expect(left[0].variant).toBe('context');
        expect(right[0].variant).toBe('context');
        expect(highlighted(left[0].segments)).toBe('');
    });

    it('marks a changed line removed on the left and added on the right with word highlights', () => {
        const { left, right } = buildLineDiff('Total income from sales', 'Total revenue from sales');
        expect(left[0].variant).toBe('removed');
        expect(left[0].marker).toBe('-');
        expect(right[0].variant).toBe('added');
        expect(right[0].marker).toBe('+');
        expect(highlighted(left[0].segments)).toBe('income');
        expect(highlighted(right[0].segments)).toBe('revenue');
    });

    it('keeps a shared first line as context and only the appended lines as added', () => {
        const existing = 'Revenue total';
        const imported = 'Revenue total\n\n```sql\nSUM(amount)\n```';
        const { left, right } = buildLineDiff(existing, imported);

        // Shared line is context (untinted) on both sides.
        expect(left[0].variant).toBe('context');
        expect(right[0].variant).toBe('context');
        // The appended code block lines are all additions on the right only.
        const addedText = right
            .filter((row) => row.variant === 'added')
            .map((row) => row.segments.map((s) => s.text).join(''))
            .join('\n');
        expect(addedText).toContain('```sql');
        expect(addedText).toContain('SUM(amount)');
        // Nothing was removed, so the left has no removed rows.
        expect(left.some((row) => row.variant === 'removed')).toBe(false);
    });

    it('renders a brand-new value as all additions with sequential line numbers', () => {
        const { left, right } = buildLineDiff('', 'line one\nline two');
        expect(left).toHaveLength(0);
        expect(right.map((r) => r.variant)).toEqual(['added', 'added']);
        expect(right.map((r) => r.num)).toEqual([1, 2]);
    });

    it('renders a cleared value as all removals', () => {
        const { left, right } = buildLineDiff('was here', '');
        expect(right).toHaveLength(0);
        expect(left[0].variant).toBe('removed');
    });
});
