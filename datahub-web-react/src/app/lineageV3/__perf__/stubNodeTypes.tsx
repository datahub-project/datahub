import React from 'react';
import { NodeProps, NodeTypes } from 'reactflow';
import styled from 'styled-components';

import { PERF_STUB_NODE_TYPE } from '@app/lineageV3/__perf__/syntheticGraph';

/**
 * Two node-component variants used by the rendering perf harness:
 *
 *  - `Light` is a minimal styled div, representative of pure ReactFlow + a
 *    React.memo'd component with cheap renders. Establishes a lower bound on
 *    per-node cost.
 *
 *  - `Heavy` mirrors the structural shape of the real `LineageEntityNode`
 *    (multiple nested styled-components, conditional sub-renders driven by
 *    props, a couple of hooks). It does NOT pull in Apollo / EntityRegistry /
 *    the entire lineage context, so it under-counts the real component, but
 *    it's a reasonable proxy for "how does a chunkier node react under load".
 */

// Stubs run in jsdom without a ThemeProvider, so colours are intentionally
// neutral CSS keywords rather than theme tokens. The perf harness asserts on
// mounted DOM-node counts; styling is purely structural.

const Wrapper = styled.div<{ $heavy?: boolean }>`
    width: 100%;
    height: 100%;
    box-sizing: border-box;
    background: transparent;
    border: 1px solid currentColor;
    border-radius: 8px;
    padding: 8px 12px;
    display: flex;
    flex-direction: column;
    gap: 4px;
    font-family: 'Roboto', sans-serif;
    font-size: 12px;
    box-shadow: ${({ $heavy }) => ($heavy ? '0 1px 3px currentColor' : 'none')};
`;

const Title = styled.div`
    font-weight: 600;
    color: inherit;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

const Subtitle = styled.div`
    color: inherit;
    font-size: 11px;
`;

const TagRow = styled.div`
    display: flex;
    gap: 4px;
    flex-wrap: wrap;
`;

const Tag = styled.span<{ $idx: number }>`
    padding: ${({ $idx }) => `${2 + ($idx % 2)}px 6px`};
    border-radius: 4px;
    background: transparent;
    border: 1px solid currentColor;
    color: inherit;
    font-size: 10px;
`;

type PerfNodeProps = NodeProps<{ urn: string; label: string }>;

const LightStubBase = ({ data }: PerfNodeProps) => (
    <Wrapper>
        <Title>{data.label}</Title>
        <Subtitle>{data.urn}</Subtitle>
    </Wrapper>
);
const LightStub = React.memo(LightStubBase);

const HeavyStubBase = ({ data, selected, dragging }: PerfNodeProps) => {
    const [open, setOpen] = React.useState(false);
    const onClick = React.useCallback(() => setOpen((v) => !v), []);
    const tags = React.useMemo(
        () => ['Owners', 'Tags', 'Domain', 'Glossary'].slice(0, (data.label.length % 4) + 1),
        [data.label],
    );
    return (
        <Wrapper $heavy onClick={onClick} aria-pressed={selected || dragging}>
            <Title>{data.label}</Title>
            <Subtitle>{data.urn}</Subtitle>
            <TagRow>
                {tags.map((tag, idx) => (
                    <Tag key={tag} $idx={idx}>
                        {tag}
                    </Tag>
                ))}
            </TagRow>
            {open && <Subtitle>Expanded · {data.urn}</Subtitle>}
        </Wrapper>
    );
};
const HeavyStub = React.memo(HeavyStubBase);

export const lightNodeTypes: NodeTypes = {
    [PERF_STUB_NODE_TYPE]: LightStub,
};

export const heavyNodeTypes: NodeTypes = {
    [PERF_STUB_NODE_TYPE]: HeavyStub,
};
