import { Pill, Tooltip } from '@components';
import React, { useEffect, useRef, useState } from 'react';
import styled from 'styled-components';

import type { ResizablePillsProps } from '@components/components/ResizablePills/types';

const Container = styled.div`
    display: flex;
    flex-wrap: nowrap;
    gap: 4px;
    width: 100%;
    align-items: center;
`;

const PillWrapper = styled.div`
    display: inline-flex;
    flex-shrink: 0;
`;

export function ResizablePills<T>({
    items,
    renderPill,
    getItemWidth,
    overflowTooltipContent,
    overflowLabel = (count) => `+${count}`,
    gap = 4,
    overflowButtonWidth = 50,
    minContainerWidthForOne = 100,
    className,
    style,
    keyExtractor,
    debounceMs = 150,
}: ResizablePillsProps<T>) {
    const containerRef = useRef<HTMLDivElement>(null);
    const [visibleCount, setVisibleCount] = useState(2);
    const timeoutRef = useRef<NodeJS.Timeout>();

    useEffect(() => {
        if (!containerRef.current || items.length === 0) return undefined;

        const resizeObserver = new ResizeObserver((entries) => {
            const [entry] = entries;
            if (!entry) return;

            // Debounce resize calculations
            if (timeoutRef.current) {
                clearTimeout(timeoutRef.current);
            }

            timeoutRef.current = setTimeout(() => {
                const { width } = entry.contentRect;

                let totalWidth = 0;
                let count = 0;

                for (let i = 0; i < items.length; i++) {
                    const itemWidth = getItemWidth(items[i]);
                    const widthWithGap = totalWidth + itemWidth + (i > 0 ? gap : 0);

                    // If this is the last item, we don't need space for overflow button
                    if (i === items.length - 1) {
                        if (widthWithGap <= width) {
                            count = i + 1;
                        }
                        break;
                    }

                    // For other items, check if we have space for this item + overflow button
                    const widthWithOverflow = widthWithGap + gap + overflowButtonWidth;
                    if (widthWithOverflow <= width) {
                        count = i + 1;
                        totalWidth = widthWithGap;
                    } else {
                        break;
                    }
                }

                // Ensure we show at least 1 item if there's reasonable space
                const finalCount = Math.max(width > minContainerWidthForOne ? 1 : 0, count);
                setVisibleCount(finalCount);
            }, debounceMs);
        });

        resizeObserver.observe(containerRef.current);

        return () => {
            if (timeoutRef.current) {
                clearTimeout(timeoutRef.current);
            }
            resizeObserver.disconnect();
        };
    }, [items, getItemWidth, gap, overflowButtonWidth, minContainerWidthForOne, debounceMs]);

    if (items.length === 0) {
        return null;
    }

    const displayItems = items.slice(0, visibleCount);
    const hiddenItems = items.slice(visibleCount);
    const hasOverflow = hiddenItems.length > 0;

    const overflowButton = hasOverflow ? (
        <Pill
            variant="outline"
            label={overflowLabel(hiddenItems.length)}
            aria-label={`${hiddenItems.length} more items hidden`}
        />
    ) : null;

    return (
        <Container ref={containerRef} className={className} style={style}>
            {displayItems.map((item, i) => (
                <PillWrapper key={keyExtractor?.(item) ?? String(i)}>{renderPill(item, i)}</PillWrapper>
            ))}

            {hasOverflow &&
                (overflowTooltipContent ? (
                    <Tooltip title={overflowTooltipContent(hiddenItems)} placement="top">
                        <span>{overflowButton}</span>
                    </Tooltip>
                ) : (
                    overflowButton
                ))}
        </Container>
    );
}
