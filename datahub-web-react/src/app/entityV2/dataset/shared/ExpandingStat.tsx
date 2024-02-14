import React, { ReactNode, useEffect, useRef, useState } from 'react';
import styled from 'styled-components';

const ExpandingStatContainer = styled.span<{ disabled: boolean; expanded: boolean; width: string }>`
    max-width: 100%;
    transition: width 250ms ease;
`;

const ExpandingStat = ({
    disabled = false,
    render,
}: {
    disabled?: boolean;

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
