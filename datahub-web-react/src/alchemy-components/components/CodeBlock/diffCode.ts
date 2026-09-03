import { diffArrays, diffLines } from 'diff';

export type CodeDiffSegmentType = 'equal' | 'removed' | 'added';

export type CodeDiffSegment = {
    type: CodeDiffSegmentType;
    value: string;
};

export type CodeDiffLineKind = 'unchanged' | 'removed' | 'added';

export type CodeDiffLine = {
    kind: CodeDiffLineKind;
    segments: CodeDiffSegment[];
};

function normalizeCode(value: string): string {
    return value.replace(/\n+$/, '');
}

/**
 * Whether read-only CodeBlock should render an inline diff instead of Prism.
 * Identical strings (ignoring trailing newlines) stay on the highlighter.
 */
export function shouldRenderCodeBlockDiff(code: string, diffAgainst?: string): boolean {
    return diffAgainst !== undefined && normalizeCode(code) !== normalizeCode(diffAgainst);
}

function splitDiffLines(value: string): string[] {
    if (!value) {
        return [];
    }
    const parts = value.split('\n');
    if (parts[parts.length - 1] === '') {
        parts.pop();
    }
    return parts;
}

function tokenizeLine(line: string): string[] {
    return line.split(/(\s+)/).filter((token) => token.length > 0);
}

function segmentType(change: { added?: boolean; removed?: boolean }): CodeDiffSegmentType {
    if (change.added) {
        return 'added';
    }
    if (change.removed) {
        return 'removed';
    }
    return 'equal';
}

function coalesceSegments(segments: CodeDiffSegment[]): CodeDiffSegment[] {
    const merged: CodeDiffSegment[] = [];
    segments.forEach((segment) => {
        const last = merged[merged.length - 1];
        if (last && last.type === segment.type) {
            last.value += segment.value;
            return;
        }
        merged.push({ ...segment });
    });

    const coalesced: CodeDiffSegment[] = [];
    let index = 0;
    while (index < merged.length) {
        const current = merged[index];
        if (current) {
            const previous = coalesced[coalesced.length - 1];
            const next = merged[index + 1];
            if (
                current.type === 'equal' &&
                /^\s+$/.test(current.value) &&
                previous &&
                next &&
                previous.type === next.type &&
                previous.type !== 'equal'
            ) {
                // Whitespace between two chunks of the same kind shouldn't split them apart.
                previous.value += current.value + next.value;
                index += 1;
            } else {
                coalesced.push(current);
            }
        }
        index += 1;
    }
    return coalesced;
}

function wordSegments(oldLine: string, newLine: string, drop: 'added' | 'removed'): CodeDiffSegment[] {
    const changes = diffArrays(tokenizeLine(oldLine), tokenizeLine(newLine));
    return coalesceSegments(
        changes
            .filter((change) => !change[drop])
            .map((change) => ({
                type: segmentType(change),
                value: (change.value ?? []).join(''),
            })),
    );
}

function hasWordChange(oldLine: string, newLine: string): boolean {
    return diffArrays(tokenizeLine(oldLine), tokenizeLine(newLine)).some((change) => change.added || change.removed);
}

/**
 * Line-level diff, with word-level chips on modified line pairs.
 * Each modification emits the old line then the new line (GitHub inline style).
 */
export function buildCodeDiff(oldCode: string, newCode: string): CodeDiffLine[] {
    const changes = diffLines(normalizeCode(oldCode), normalizeCode(newCode));
    const lines: CodeDiffLine[] = [];
    let index = 0;

    while (index < changes.length) {
        const change = changes[index];
        if (!change) {
            break;
        }

        if (!change.added && !change.removed) {
            splitDiffLines(change.value).forEach((line) => {
                lines.push({ kind: 'unchanged', segments: [{ type: 'equal', value: line }] });
            });
            index += 1;
        } else {
            const removed: string[] = [];
            const added: string[] = [];
            while (index < changes.length) {
                const hunk = changes[index];
                if (!hunk?.added && !hunk?.removed) {
                    break;
                }
                const hunkLines = splitDiffLines(hunk?.value ?? '');
                if (hunk?.removed) {
                    removed.push(...hunkLines);
                } else if (hunk?.added) {
                    added.push(...hunkLines);
                }
                index += 1;
            }

            const paired = Math.min(removed.length, added.length);
            for (let pairIndex = 0; pairIndex < paired; pairIndex++) {
                const previous = removed[pairIndex] ?? '';
                const next = added[pairIndex] ?? '';
                if (hasWordChange(previous, next)) {
                    lines.push({ kind: 'removed', segments: wordSegments(previous, next, 'added') });
                    lines.push({ kind: 'added', segments: wordSegments(previous, next, 'removed') });
                } else {
                    lines.push({ kind: 'unchanged', segments: [{ type: 'equal', value: next }] });
                }
            }
            for (let leftover = paired; leftover < removed.length; leftover++) {
                lines.push({ kind: 'removed', segments: [{ type: 'removed', value: removed[leftover] ?? '' }] });
            }
            for (let leftover = paired; leftover < added.length; leftover++) {
                lines.push({ kind: 'added', segments: [{ type: 'added', value: added[leftover] ?? '' }] });
            }
        }
    }

    return lines;
}
