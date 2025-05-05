import React, { useEffect, useRef, useState } from 'react';
import styled from 'styled-components';

import { Tooltip2 } from '@components/components/Tooltip2';

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
        <Tooltip2 title={isTruncated ? text : undefined}>
            <TextWrapper ref={textRef}>{text}</TextWrapper>
        </Tooltip2>
    );
};
