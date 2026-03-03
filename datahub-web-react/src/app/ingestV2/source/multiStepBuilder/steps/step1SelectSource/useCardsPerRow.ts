import { useEffect, useState } from 'react';

export function useCardsPerRow(
    containerRef: React.RefObject<HTMLElement>,
    cardMinWidth: number,
    gap = 8,
    minColumns = 1,
) {
    const [cardsPerRow, setCardsPerRow] = useState<number>(minColumns);

    useEffect(() => {
        const element = containerRef.current;
        if (!element) return undefined;

        const computeCardsPerRow = () => {
            const containerWidth = Math.max(0, element.clientWidth);

            // Calculate the number of cards that can fit based on card width and spacing
            const spacePerCard = cardMinWidth + gap;
            const possibleColumns = Math.floor((containerWidth + gap) / spacePerCard);

            const columns = Math.max(minColumns, possibleColumns);
            setCardsPerRow(columns);
        };

        computeCardsPerRow();

        let resizeObserver: ResizeObserver | undefined;

        if (typeof ResizeObserver !== 'undefined') {
            resizeObserver = new ResizeObserver(computeCardsPerRow);
            resizeObserver.observe(element);
        } else {
            window.addEventListener('resize', computeCardsPerRow);
        }

        return () => {
            if (resizeObserver) {
                resizeObserver.disconnect();
            } else {
                window.removeEventListener('resize', computeCardsPerRow);
            }
        };
    }, [containerRef, cardMinWidth, gap, minColumns]);

    return cardsPerRow;
}
