import React, { ReactNode, useEffect, useRef, useState } from 'react';
import styled from 'styled-components';

const ExpandingStatContainer = styled.span<{ disabled: boolean; expanded: boolean; width: string; renderCss: boolean }>`
    overflow: ${(props) => props.renderCss && 'hidden'};
    white-space: ${(props) => props.renderCss && 'nowrap'};
    max-width: ${(props) => (!props.renderCss ? '100%' : props.width)};
    transition: width 250ms ease;
`;

const ExpandingStat = ({
    disabled = false,
    render,
    renderCss = true,
}: {
    disabled?: boolean;
    renderCss?: boolean;
    render: (isExpanded: boolean) => ReactNode;
}) => {
    const contentRef = useRef<HTMLSpanElement>(null);
    const [width, setWidth] = useState<string>('inherit');
    const [isExpanded, setIsExpanded] = useState(false);

    useEffect(() => {
        if (!contentRef.current) return;
        setWidth(`${contentRef.current.offsetWidth}px`);
    }, [isExpanded]);

    const onMouseEnter = () => {
        if (!disabled) setIsExpanded(true);
    };

    const onMouseLeave = () => {
        if (!disabled) setIsExpanded(false);
    };

    return (
        <ExpandingStatContainer
            renderCss={renderCss}
            disabled={disabled}
            expanded={isExpanded}
            width={width}
            onMouseEnter={onMouseEnter}
            onMouseLeave={onMouseLeave}
        >
            <span ref={contentRef}>{render(isExpanded)}</span>
        </ExpandingStatContainer>
    );
};

export default ExpandingStat;
