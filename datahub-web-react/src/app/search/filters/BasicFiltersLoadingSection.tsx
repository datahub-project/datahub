import { Skeleton } from 'antd';
import * as React from 'react';
import styled from 'styled-components';

const Container = styled.div`
    display: flex;
    align-items: center;
`;

const CardSkeleton = styled(Skeleton.Input)`
    && {
        padding: 2px 12px 2px 0px;
        height: 32px;
        border-radius: 8px;
    }
`;

export default function BasicFiltersLoadingSection() {
    return (
        <Container>
            <CardSkeleton active size="default" />
            <CardSkeleton active size="default" />
            <CardSkeleton active size="default" />
            <CardSkeleton active size="default" />
        </Container>
    );
}
