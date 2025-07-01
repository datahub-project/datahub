import React, { useCallback, useState } from 'react';
import styled from 'styled-components';

const Wrapper = styled.div<{ height: number | undefined; defaultHeight: number }>`
    font-size: ${({ height, defaultHeight }) => Math.min(height ?? defaultHeight, defaultHeight)}px;
`;

interface Props extends React.HTMLAttributes<HTMLDivElement> {
    defaultHeight: number;
    children: React.ReactNode;
    className?: string;
}

export default function MatchTextSizeWrapper({ defaultHeight, children, className, ...rest }: Props) {
    const [height, setHeight] = useState<number | undefined>(undefined);
    const ref = useCallback((node: HTMLDivElement) => {
        if (node) {
            const resizeObserver = new ResizeObserver(() => {
                setHeight(node.clientHeight);
            });
            resizeObserver.observe(node);
            return () => resizeObserver.disconnect();
        }
        return () => {};
    }, []);

    return (
        <Wrapper ref={ref} className={className} height={height} defaultHeight={defaultHeight} {...rest}>
            {children}
        </Wrapper>
    );
}
