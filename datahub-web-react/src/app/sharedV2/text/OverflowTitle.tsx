import OptionalTooltip from '@app/sharedV2/ant/OptionalTooltip';
import { TooltipProps } from 'antd';
import React, { useEffect } from 'react';
import styled from 'styled-components';
import Highlight from 'react-highlighter';

const Wrapper = styled.div<{ scale: number; computedRatio: boolean }>`
    // Wrap up to two lines, shrinking text as needed
    font-size: ${({ scale }) => `${scale}em`} !important;
    line-height: 1.25;
    max-height: 2.5em; // 2 lines with 1.25 line height
    overflow: hidden;
    white-space: ${({ computedRatio }) => (computedRatio ? 'normal' : 'nowrap')};
    word-break: break-all;

    // Position at start, vertically, as parent aligns center
    display: flex;
    align-items: start;
    height: 100%;

    mark {
        padding: 0;
    }
`;

const MIN_SCALE = 2 / 3;
const TOOLTIP_THRESHOLD = 0.8; // Show tooltip if text is smaller than TOOLTIP_THRESHOLD em

interface Props {
    title?: string;
    highlightText?: string;
    highlightColor?: string;
    className?: string;
    placement?: TooltipProps['placement'];
}

export default function OverflowTitle({ title, highlightText, highlightColor, className, placement = 'top' }: Props) {
    const [scale, setScale] = React.useState<number>(1);
    const [ratio, setRatio] = React.useState<number | undefined>(undefined);

    useEffect(() => {
        setScale(1);
    }, [title]);

    useEffect(() => {
        if (ratio && ratio > 1) {
            setScale(Math.max(MIN_SCALE, Math.min(1, 2 / ratio - 0.03)));
        }
    }, [title, ratio]);

    const ref = React.useCallback((node: HTMLDivElement) => {
        if (node !== null) {
            setRatio((oldRatio) => oldRatio ?? node.scrollWidth / node.clientWidth);
        }
    }, []);

    return (
        <OptionalTooltip tooltipProps={{ title, placement }} enabled={scale < TOOLTIP_THRESHOLD}>
            <Wrapper className={className} ref={ref} scale={scale} computedRatio={!!ratio}>
                <Highlight search={highlightText} matchStyle={{ backgroundColor: highlightColor }}>
                    {title}
                </Highlight>
            </Wrapper>
        </OptionalTooltip>
    );
}
