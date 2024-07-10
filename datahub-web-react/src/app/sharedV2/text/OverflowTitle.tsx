import OptionalTooltip from '@app/sharedV2/ant/OptionalTooltip';
import { TooltipProps } from 'antd';
import React, { useEffect } from 'react';
import styled from 'styled-components';

const Wrapper = styled.div<{ scale: number }>`
    font-size: ${({ scale }) => `${scale}em`} !important;
    max-height: 15px;

    overflow: hidden;
    white-space: ${({ scale }) => (scale <= 0.5 ? 'normal' : 'nowrap')};
    word-break: break-all;
`;

const LINE_THRESHOLDS = [2, Infinity]; // Series is [1*2, 2*3, 3*4, ...]
const MIN_SCALE = 1 / LINE_THRESHOLDS.length; // Minimum text size, in em
const TOOLTIP_THRESHOLD = 0.75; // Show tooltip if text is smaller than TOOLTIP_THRESHOLD em
const TRUNCATED_MAX_CHARACTERS = 75; // Truncate text to this many characters, if text overflows vertically

interface Props {
    title?: string;
    className?: string;
    placement?: TooltipProps['placement'];
}

export default function OverflowTitle({ title, className, placement = 'top' }: Props) {
    const [scale, setScale] = React.useState<number>(1);
    const [ratio, setRatio] = React.useState<number>(1);
    const [truncated, setTruncated] = React.useState<boolean>(false);

    useEffect(() => {
        setScale(1);
    }, [title]);

    useEffect(() => {
        if (ratio > 1) {
            const numLines = LINE_THRESHOLDS.findIndex((v) => v >= ratio) + 1;
            setScale(Math.max(MIN_SCALE, Math.min(1 / numLines, numLines / ratio - 0.01)));
        }
    }, [ratio]);

    const ref = React.useCallback(
        (node: HTMLDivElement) => {
            if (node !== null) {
                if (scale === 1) {
                    setRatio(node.scrollWidth / node.clientWidth);
                }
                setTruncated(node.scrollHeight > node.clientHeight);
            }
        },
        [scale],
    );

    return (
        <OptionalTooltip tooltipProps={{ title, placement }} enabled={scale < TOOLTIP_THRESHOLD}>
            <Wrapper className={className} ref={ref} scale={scale}>
                {truncated ? `${title?.substring(0, TRUNCATED_MAX_CHARACTERS)}...` : title}
            </Wrapper>
        </OptionalTooltip>
    );
}
