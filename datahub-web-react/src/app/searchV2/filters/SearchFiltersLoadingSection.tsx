import { Skeleton } from '@components';
import * as React from 'react';
import styled from 'styled-components';

const Container = styled.div`
    display: flex;
    gap: 12px;
`;

interface Props {
    noOfLoadingSkeletons?: number;
}

export default function SearchFiltersLoadingSection({ noOfLoadingSkeletons = 4 }: Props) {
    return (
        <Container>
            {Array.from({ length: noOfLoadingSkeletons }).map((_, index) => (
                // eslint-disable-next-line react/no-array-index-key -- static placeholders
                <Skeleton.Input key={index} active height={32} width={120} />
            ))}
        </Container>
    );
}
