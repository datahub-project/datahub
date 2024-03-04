import React from 'react';
import styled from 'styled-components';
import { useInView } from 'react-intersection-observer';

const VirtualChildWrapper = styled.div<{ $inView: boolean; $height: number }>`
    height: ${(props) => (props.$inView ? 'auto' : `${props.$height}px`)};
    ${(props) => !props.$inView && 'visiblity: hidden;'}
`;

interface VirtualProps {
    height: number;
    children: React.ReactNode;
    triggerOnce?: boolean;
}

export default function VirtualScrollChild({ height, children, triggerOnce }: VirtualProps) {
    const [ref, inView] = useInView({ triggerOnce });

    return (
        <VirtualChildWrapper $inView={inView} $height={height} ref={ref}>
            {inView ? children : null}
        </VirtualChildWrapper>
    );
}
