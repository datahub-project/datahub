import React from 'react';

import {
    CodeBlockDiffAdded,
    CodeBlockDiffEqual,
    CodeBlockDiffPre,
    CodeBlockDiffRemoved,
} from '@components/components/CodeBlock/components';
import type { CodeDiffLine, CodeDiffSegment } from '@components/components/CodeBlock/diffCode';

type Props = {
    lines: CodeDiffLine[];
    wrap: boolean;
    dataTestId?: string;
};

function renderSegment(segment: CodeDiffSegment, key: number): React.ReactNode {
    if (segment.type === 'removed') {
        return <CodeBlockDiffRemoved key={key}>{segment.value}</CodeBlockDiffRemoved>;
    }
    if (segment.type === 'added') {
        return <CodeBlockDiffAdded key={key}>{segment.value}</CodeBlockDiffAdded>;
    }
    return <CodeBlockDiffEqual key={key}>{segment.value}</CodeBlockDiffEqual>;
}

/**
 * Read-only inline word diff. Uses `del` / `ins` so removals stay announced
 * without relying on color alone.
 */
export function CodeBlockDiff({ lines, wrap, dataTestId }: Props): React.ReactElement {
    return (
        <CodeBlockDiffPre $wrap={wrap} data-testid={dataTestId}>
            {lines.map((line, lineIndex) => (
                // A diff line's identity is its position; the list is rebuilt whole and never reordered.
                // eslint-disable-next-line react/no-array-index-key
                <React.Fragment key={lineIndex}>
                    {lineIndex > 0 ? '\n' : null}
                    {line.segments.map((segment, segmentIndex) => renderSegment(segment, segmentIndex))}
                </React.Fragment>
            ))}
        </CodeBlockDiffPre>
    );
}
