import { DataContext, Margin } from '@visx/xychart';
import { RefObject, useContext, useEffect, useMemo } from 'react';

interface DynamicMarginSetterProps {
    setMargin: (margin: Margin) => void;
    wrapperRef: RefObject<HTMLDivElement>;
    minimalMargin?: Partial<Margin>;
    // for a case when component is not wrapped with XYChart
    currentMargin?: Partial<Margin>;
}

const GAP_BETWEEN_LEFT_AXIS_AND_LABEL = 6;

export default function DynamicMarginSetter({
    setMargin,
    wrapperRef,
    minimalMargin,
    currentMargin,
}: DynamicMarginSetterProps) {
    const { margin } = useContext(DataContext);

    const safeMargin = useMemo(
        () => ({
            top: (currentMargin?.top ?? 0) + (margin?.top ?? 0),
            right: (currentMargin?.right ?? 0) + (margin?.right ?? 0),
            bottom: (currentMargin?.bottom ?? 0) + (margin?.bottom ?? 0),
            left: (currentMargin?.left ?? 0) + (margin?.left ?? 0),
        }),
        [margin, currentMargin],
    );

    useEffect(() => {
        const adjustMargin = () => {
            const wrapper = wrapperRef.current;
            if (!wrapper) return;

            let { left: leftMargin, right: rightMargin } = safeMargin;

            // Adjust margins based on bottom axis labels
            const bottomLabelElements = Array.from(wrapper.querySelectorAll<SVGGraphicsElement>('.bottom-axis-tick'));
            if (bottomLabelElements.length > 0) {
                const firstLabel = bottomLabelElements[0].getBBox();
                if (firstLabel.x < 0) {
                    leftMargin += Math.abs(firstLabel.x);
                }

                const content = wrapper.querySelector<SVGGraphicsElement>('.content-group');
                if (content) {
                    const contentRect = content.getBBox();
                    const lastBottomLabel = bottomLabelElements[bottomLabelElements.length - 1].getBBox();

                    const contentRightX = contentRect.x + contentRect.width;
                    const lastBottomLabelRightX = lastBottomLabel.x + lastBottomLabel.width;

                    if (lastBottomLabelRightX > contentRightX) {
                        rightMargin = Math.max(rightMargin, lastBottomLabelRightX - contentRightX);
                    }
                }
            }

            // Adjust margins based on left axis labels
            const leftAxis = wrapper.querySelector<SVGGraphicsElement>('.left-axis');
            if (leftAxis) {
                const axisWidth = leftAxis.getBBox().width + GAP_BETWEEN_LEFT_AXIS_AND_LABEL;
                leftMargin = Math.max(leftMargin, axisWidth);
            }

            const updatedMargin: Margin = {
                top: Math.max(minimalMargin?.top ?? 0, Math.ceil(safeMargin.top)),
                right: Math.max(minimalMargin?.right ?? 0, Math.ceil(rightMargin)),
                bottom: Math.max(minimalMargin?.bottom ?? 0, Math.ceil(safeMargin.bottom)),
                left: Math.max(minimalMargin?.left ?? 0, Math.ceil(leftMargin)),
            };

            setMargin(updatedMargin);
        };

        const timeout = setTimeout(adjustMargin);
        return () => clearTimeout(timeout);
    }, [wrapperRef, setMargin, minimalMargin, safeMargin]);

    return null;
}
