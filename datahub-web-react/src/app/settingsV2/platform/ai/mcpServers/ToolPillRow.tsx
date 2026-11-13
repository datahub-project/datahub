import React, { useLayoutEffect, useRef, useState } from 'react';
import styled from 'styled-components';

import { ToolItemPill } from '@app/settingsV2/platform/ai/mcpServers/ToolItemPill';
import { countFittingPills } from '@app/settingsV2/platform/ai/mcpServers/ToolPillRow.utils';
import { type EnabledToolItem } from '@app/settingsV2/platform/ai/mcpServers/utils/mcpServerDataUtils';
import { Pill, Tooltip } from '@src/alchemy-components';

const GAP = 4;

const Row = styled.div`
    position: relative;
    display: flex;
    flex-wrap: nowrap;
    align-items: center;
    gap: ${GAP}px;
    width: 100%;
    min-width: 0;
    overflow: hidden;
`;

// Every pill, laid out off-screen at its natural width, so the row can tell how many fit.
const MeasureRow = styled.div`
    position: absolute;
    top: 0;
    left: 0;
    display: flex;
    flex-wrap: nowrap;
    gap: ${GAP}px;
    visibility: hidden;
    pointer-events: none;
`;

const Wrapped = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: ${GAP}px;
    max-width: 300px;
`;

// antd Tooltip binds hover to its child; an inline-flex wrapper gives the +N pill a box to hover over.
const TooltipTarget = styled.span`
    display: inline-flex;
    flex-shrink: 0;
`;

const PillSlot = styled.span`
    display: inline-flex;
    flex-shrink: 0;
`;

type ToolPillRowProps = {
    items: EnabledToolItem[];
};

const itemKey = (item: EnabledToolItem) => item.agentUrn ?? item.label;

/** Tool pills on one line, as many as fit the width; the rest go behind a "+N" pill. */
export function ToolPillRow({ items }: ToolPillRowProps) {
    const rowRef = useRef<HTMLDivElement>(null);
    const measureRef = useRef<HTMLDivElement>(null);
    const [visibleCount, setVisibleCount] = useState(items.length);

    useLayoutEffect(() => {
        const row = rowRef.current;
        const measure = measureRef.current;
        if (!row || !measure) return undefined;
        const update = () => {
            const widths = Array.from(measure.children, (child) => (child as HTMLElement).offsetWidth);
            // The last measured child is a "+N" pill sized for the longest count.
            const moreWidth = widths.pop() ?? 0;
            setVisibleCount(countFittingPills(widths, row.clientWidth, GAP, moreWidth));
        };
        update();
        const observer = new ResizeObserver(update);
        observer.observe(row);
        return () => observer.disconnect();
    }, [items]);

    const visible = items.slice(0, visibleCount);
    const hidden = items.slice(visibleCount);

    return (
        <Row ref={rowRef} data-testid="mcp-tool-pill-row">
            <MeasureRow ref={measureRef} aria-hidden>
                {items.map((item) => (
                    <PillSlot key={itemKey(item)}>
                        <ToolItemPill item={item} />
                    </PillSlot>
                ))}
                <PillSlot>
                    <Pill label={`+${items.length}`} color="gray" size="sm" />
                </PillSlot>
            </MeasureRow>
            {visible.map((item) => (
                <PillSlot key={itemKey(item)}>
                    <ToolItemPill item={item} />
                </PillSlot>
            ))}
            {hidden.length > 0 && (
                <Tooltip
                    title={
                        <Wrapped>
                            {hidden.map((item) => (
                                <ToolItemPill key={itemKey(item)} item={item} />
                            ))}
                        </Wrapped>
                    }
                >
                    <TooltipTarget>
                        <Pill label={`+${hidden.length}`} color="gray" size="sm" clickable />
                    </TooltipTarget>
                </Tooltip>
            )}
        </Row>
    );
}
