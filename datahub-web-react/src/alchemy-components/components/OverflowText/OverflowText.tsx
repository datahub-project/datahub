import React, { useEffect, useRef, useState } from 'react';
import styled from 'styled-components';

import { Tooltip } from '@components/components/Tooltip';

const TextWrapper = styled.span`
    display: block;
    max-width: 100%;
    overflow: hidden;
    text-overflow: ellipsis;
`;

interface Props {
    text: string;
}

export const OverflowText = ({ text }: Props) => {
    const textRef = useRef<HTMLSpanElement>(null);
    const [isTruncated, setIsTruncated] = useState(false);

    useEffect(() => {
        const el = textRef.current;
        if (el) {
            setIsTruncated(el.scrollWidth > el.clientWidth);
        }
    }, [text]);

    return (
        <Tooltip title={isTruncated ? text : undefined}>
            <TextWrapper ref={textRef}>{text}</TextWrapper>
        </Tooltip>
    );
};
