import React, { useEffect, useRef, useState } from 'react';
import styled from 'styled-components';

import { Tooltip, TooltipPlacement } from '@components/components/Tooltip';

const TextWrapper = styled.span`
    display: block;
    max-width: 100%;
    overflow: hidden;
    text-overflow: ellipsis;
`;

type Props = {
    /** Plain text to render. Also the default tooltip content when truncated. */
    text?: string;
    /** Rich content to render instead of `text` (e.g. highlighted matches or translated markup). */
    children?: React.ReactNode;
    /** Tooltip content when truncated. Defaults to `text`, or `children` when `text` is not set. */
    tooltip?: React.ReactNode;
    /** Set to false to truncate without ever showing a tooltip. */
    showTooltip?: boolean;
    tooltipPlacement?: TooltipPlacement;
    showArrow?: boolean;
    className?: string;
};

export const OverflowText = ({
    text,
    children,
    tooltip,
    showTooltip = true,
    tooltipPlacement,
    showArrow,
    className,
}: Props) => {
    const textRef = useRef<HTMLSpanElement>(null);
    const [isTruncated, setIsTruncated] = useState(false);

    useEffect(() => {
        const el = textRef.current;
        if (el) {
            setIsTruncated(el.scrollWidth > el.clientWidth);
        }
    }, [text, children]);

    // Width changes (parent resize, sidebar toggle) can truncate text without its content changing.
    useEffect(() => {
        const el = textRef.current;
        if (!el || typeof ResizeObserver === 'undefined') return undefined;
        const observer = new ResizeObserver(() => setIsTruncated(el.scrollWidth > el.clientWidth));
        observer.observe(el);
        return () => observer.disconnect();
    }, []);

    const content = children ?? text;
    const tooltipContent = tooltip ?? text ?? children;

    return (
        <Tooltip
            title={showTooltip && isTruncated ? tooltipContent : undefined}
            placement={tooltipPlacement}
            showArrow={showArrow}
        >
            <TextWrapper ref={textRef} className={className}>
                {content}
            </TextWrapper>
        </Tooltip>
    );
};
