/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
