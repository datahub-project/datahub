import { diffLines, diffWords } from 'diff';

export type DiffVariant = 'removed' | 'added' | 'context';

export interface Segment {
    text: string;
    highlighted: boolean;
}

export interface DiffRow {
    num: number;
    marker: string;
    variant: DiffVariant;
    segments: Segment[];
}

function ensureTrailingNewline(value: string): string {
    // Empty stays empty so an absent value produces no phantom line; non-empty gets a trailing
    // newline so an otherwise-identical final line matches (and renders as context, not changed).
    if (value === '' || value.endsWith('\n')) return value;
    return `${value}\n`;
}

function splitToLines(value: string): string[] {
    const v = value.endsWith('\n') ? value.slice(0, -1) : value;
    return v.split('\n');
}

function partVariant(part: { added?: boolean; removed?: boolean }): DiffVariant {
    if (part.added) return 'added';
    if (part.removed) return 'removed';
    return 'context';
}

/**
 * Word-level segments for one side of a changed line/value. side='left' keeps removed+common
 * tokens (highlighting removals); side='right' keeps added+common (highlighting additions).
 */
export function wordSegments(removedText: string, addedText: string, side: 'left' | 'right'): Segment[] {
    const parts = diffWords(removedText, addedText);
    return parts
        .filter((p) => (side === 'left' ? !p.added : !p.removed))
        .map((p) => ({ text: p.value, highlighted: side === 'left' ? !!p.removed : !!p.added }));
}

/**
 * Build a side-by-side line diff. Identical lines become untrimmed `context` rows on both sides;
 * a removed block adjacent to an added block is treated as a modification and diffed word-by-word
 * line-for-line so only the changed words are highlighted.
 */
export function buildLineDiff(existingText: string, importedText: string): { left: DiffRow[]; right: DiffRow[] } {
    const parts = diffLines(ensureTrailingNewline(existingText || ''), ensureTrailingNewline(importedText || ''));
    const blocks = parts.map((p) => ({ type: partVariant(p), lines: splitToLines(p.value) }));

    const left: DiffRow[] = [];
    const right: DiffRow[] = [];
    const pushLeft = (marker: string, variant: DiffVariant, segments: Segment[]) =>
        left.push({ num: left.length + 1, marker, variant, segments });
    const pushRight = (marker: string, variant: DiffVariant, segments: Segment[]) =>
        right.push({ num: right.length + 1, marker, variant, segments });

    let i = 0;
    while (i < blocks.length) {
        const block = blocks[i];
        const next = blocks[i + 1];
        if (block.type === 'context') {
            block.lines.forEach((line) => {
                pushLeft(' ', 'context', [{ text: line, highlighted: false }]);
                pushRight(' ', 'context', [{ text: line, highlighted: false }]);
            });
            i += 1;
        } else if (block.type === 'removed' && next && next.type === 'added') {
            const maxLen = Math.max(block.lines.length, next.lines.length);
            for (let k = 0; k < maxLen; k += 1) {
                const rem = block.lines[k];
                const add = next.lines[k];
                if (rem !== undefined && add !== undefined) {
                    pushLeft('-', 'removed', wordSegments(rem, add, 'left'));
                    pushRight('+', 'added', wordSegments(rem, add, 'right'));
                } else if (rem !== undefined) {
                    pushLeft('-', 'removed', [{ text: rem, highlighted: true }]);
                } else if (add !== undefined) {
                    pushRight('+', 'added', [{ text: add, highlighted: true }]);
                }
            }
            i += 2;
        } else if (block.type === 'removed') {
            block.lines.forEach((line) => pushLeft('-', 'removed', [{ text: line, highlighted: true }]));
            i += 1;
        } else {
            block.lines.forEach((line) => pushRight('+', 'added', [{ text: line, highlighted: true }]));
            i += 1;
        }
    }
    return { left, right };
}
