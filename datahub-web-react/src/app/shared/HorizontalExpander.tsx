import React, { ReactNode, useEffect, useRef, useState } from 'react';
import styled from 'styled-components';

type Props = {
    disabled?: boolean;
    render: (isExpanded: boolean) => ReactNode;
};

const TRANSITION_TIMING = 250;

const ExpandingStatContainer = styled.span<{ disabled: boolean; overflow: 'visible' | 'hidden'; width: string }>`
    overflow: ${(props) => props.overflow};
    white-space: nowrap;
    width: ${(props) => props.width};
    transition: width ${TRANSITION_TIMING}ms ease;
`;

const HorizontalExpander = ({ disabled = false, render }: Props) => {
    const contentRef = useRef<HTMLSpanElement>(null);
    const [width, setWidth] = useState<string>('inherit');
    const [isExpanded, setIsExpanded] = useState(false);
    const [overflow, setOverflow] = useState<'visible' | 'hidden'>('hidden');

    useEffect(() => {
        if (!contentRef.current) return () => {};

        setWidth(`${contentRef.current.offsetWidth}px`);

        const timer = window.setTimeout(() => {
            setOverflow(isExpanded ? 'visible' : 'hidden');
        }, TRANSITION_TIMING);

        return () => {
            window.clearTimeout(timer);
        };
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
            overflow={overflow}
            width={width}
            onMouseEnter={onMouseEnter}
            onMouseLeave={onMouseLeave}
        >
            <span ref={contentRef}>{render(isExpanded)}</span>
        </ExpandingStatContainer>
    );
};

export default HorizontalExpander;
