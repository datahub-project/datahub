import { Skeleton } from 'antd';
import * as React from 'react';
import styled from 'styled-components';

const Container = styled.div`
    display: flex;
`;

const CardSkeleton = styled(Skeleton.Input)`
    && {
        padding: 2px 12px 2px 0px;
        height: 32px;
        border-radius: 8px;
    }
`;

interface Props {
    noOfLoadingSkeletons?: number;
}

export default function SearchFiltersLoadingSection({ noOfLoadingSkeletons = 4 }: Props) {
    return (
        <Container>
            {Array.from({ length: noOfLoadingSkeletons }).map(() => (
                <CardSkeleton active size="default" />
            ))}
        </Container>
    );
}
