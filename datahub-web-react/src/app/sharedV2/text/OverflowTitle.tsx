import React, { useEffect, useRef, useState } from 'react';
import styled from 'styled-components';

const Wrapper = styled.span`
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

export default function OverflowTitle({ title, className }: { title?: string; className?: string }) {
    const [showTitle, setShowTitle] = useState(false);

    const ref = useRef<HTMLSpanElement>(null);

    useEffect(() => {
        if (ref.current && title) {
            const node = ref.current;
            const resizeObserver = new ResizeObserver(() => {
                setShowTitle(node.scrollWidth > node.clientWidth);
            });
            resizeObserver.observe(node);
            return () => resizeObserver.disconnect();
        }
        return () => {};
    }, [title]);

    return (
        <Wrapper ref={ref} className={className} title={showTitle ? title : undefined}>
            {title}
        </Wrapper>
    );
}
