import React, { useEffect } from 'react';
import { useInView } from 'react-intersection-observer';
import styled from 'styled-components';

import Loading from '@app/shared/Loading';

const ObserverContainer = styled.div`
    height: 1px;
    margin-top: 1px;
`;

const LoadMoreContainer = styled.div<{ $level: number }>`
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 4px 8px 4px ${(props) => 8 + props.$level * 16}px;
    min-height: 28px;
`;

interface ChildLoadMoreTriggerProps {
    parentUrn: string;
    level: number;
    loading: boolean;
    onLoad: (parentUrn: string) => void;
}

/**
 * Invisible trigger that auto-loads the next page of a parent's children when
 * scrolled into view. Renders a spinner in place while the page is loading.
 */
export function ChildLoadMoreTrigger({ parentUrn, level, loading: isLoading, onLoad }: ChildLoadMoreTriggerProps) {
    const [ref, inView] = useInView();

    useEffect(() => {
        if (inView && !isLoading) {
            onLoad(parentUrn);
        }
    }, [inView, isLoading, parentUrn, onLoad]);

    if (isLoading) {
        return (
            <LoadMoreContainer $level={level}>
                <Loading height={12} />
            </LoadMoreContainer>
        );
    }

    return <ObserverContainer ref={ref} />;
}
