import ResizeObserver from 'rc-resize-observer';
import React, { useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';

const Container = styled.div<{
    $gap?: number;
    $shouldFillAllAvailableSpace?: boolean;
    $justifyContent: 'end' | 'start';
    $alignItems?: React.CSSProperties['alignItems'];
}>`
    display: flex;
    flex-direction: row;
    ${(props) => props.$shouldFillAllAvailableSpace && `justify-content: ${props.$justifyContent};`}
    width: ${(props) => (props.$shouldFillAllAvailableSpace ? '100%' : 'fit-content')};
    overflow: hidden;
    ${(props) => props.$gap !== undefined && `gap: ${props.$gap}px;`}
    ${(props) => props.$alignItems !== undefined && `align-items: ${props.$alignItems};`}
`;

const HiddenContainer = styled.div`
    position: absolute;
    visibility: hidden;
    pointer-events: none;
`;

const FitContainer = styled.div`
    width: fit-content;
`;

export interface OverflowListItem {
    key: string;
    node: React.ReactNode;
}

interface Props<Item extends OverflowListItem> {
    items: Item[];
    renderHiddenItems: (items: Item[]) => React.ReactNode;
    gap?: number;
    shouldFillAllAvailableSpace?: boolean;
    justifyContent?: 'start' | 'end';
    alignItems?: React.CSSProperties['alignItems'];
}

export default function OverflowList<Item extends OverflowListItem>({
    items,
    renderHiddenItems,
    gap,
    shouldFillAllAvailableSpace = true,
    justifyContent = 'end',
    alignItems,
}: Props<Item>) {
    const [itemsWidths, setItemsWidth] = useState<Map<string, number>>(new Map());
    const [containerWidth, setContainerWidth] = useState<number>(0);
    const [renderedHiddenItemsWidth, setRenderedHiddenItemsWidth] = useState<number>(0);
    const [visibleItems, setVisibleItems] = useState<Item[]>([]);
    const [hiddenItems, setHiddenItems] = useState<Item[]>([]);
    const finalGap = useMemo(() => gap ?? 0, [gap]);

    const onItemResize = (key: string, width: number) => {
        setItemsWidth((prev) => new Map([...prev, [key, width]]));
    };

    const onContainerResize = (newWidth: number) => setContainerWidth(newWidth);

    const onRenderedHiddenItemsResize = (newWidth: number) => setRenderedHiddenItemsWidth(newWidth);

    useEffect(() => {
        const updateVisibleAndHiddenItems = () => {
            if (items.length === 0) {
                setVisibleItems([]);
                setHiddenItems([]);
                return;
            }

            // Should show at least one item
            if (items.length === 1) {
                setVisibleItems(items);
                setHiddenItems([]);
                return;
            }

            const neededSpaceForAllItems = items.reduce(
                (accumulatedWidth, item) => accumulatedWidth + (itemsWidths.get(item.key) ?? 0) + finalGap,
                0,
            );

            // Container has enough space to render all items
            if (neededSpaceForAllItems <= containerWidth) {
                setVisibleItems(items);
                setHiddenItems([]);
                return;
            }

            const [firstItem, ...restOfItems] = items;

            const firstItemWidth = itemsWidths.get(firstItem.key) ?? 0;

            // Should show at least one item
            const newVisibleItems: Item[] = [firstItem];
            const newHiddenItems: Item[] = [];

            // compute available width considering that the first item and container with hidden items shlould be rendered to
            const availableWidth =
                containerWidth - firstItemWidth - finalGap - (renderedHiddenItemsWidth ?? 0) - finalGap;

            let accumulatedWidth = 0;

            restOfItems.forEach((item) => {
                const width = itemsWidths.get(item.key) ?? 0;

                // Skip items without any width
                if (width === 0) return;

                accumulatedWidth += width + finalGap;

                if (accumulatedWidth > availableWidth) {
                    newHiddenItems.push(item);
                } else {
                    newVisibleItems.push(item);
                }
            });

            setVisibleItems(newVisibleItems);
            setHiddenItems(newHiddenItems);
        };

        if (containerWidth !== undefined) {
            updateVisibleAndHiddenItems();
        }
    }, [containerWidth, itemsWidths, renderedHiddenItemsWidth, items, finalGap]);

    return (
        <Container $shouldFillAllAvailableSpace={shouldFillAllAvailableSpace} $justifyContent={justifyContent}>
            <ResizeObserver onResize={(size) => onContainerResize(size.width)}>
                <Container
                    $gap={finalGap}
                    $shouldFillAllAvailableSpace={shouldFillAllAvailableSpace}
                    $justifyContent={justifyContent}
                    $alignItems={alignItems}
                >
                    <HiddenContainer>
                        {items.map((item) => (
                            <ResizeObserver
                                onResize={(size) => onItemResize(item.key, size.offsetWidth)}
                                key={item.key}
                            >
                                <FitContainer>{item.node}</FitContainer>
                            </ResizeObserver>
                        ))}
                        <ResizeObserver onResize={(size) => onRenderedHiddenItemsResize(size.offsetWidth)}>
                            <FitContainer>{renderHiddenItems?.([])}</FitContainer>
                        </ResizeObserver>
                    </HiddenContainer>
                    {visibleItems.map((item) => (
                        <React.Fragment key={item.key}>{item.node}</React.Fragment>
                    ))}
                    {hiddenItems.length > 0 && renderHiddenItems?.(hiddenItems)}
                </Container>
            </ResizeObserver>
        </Container>
    );
}
