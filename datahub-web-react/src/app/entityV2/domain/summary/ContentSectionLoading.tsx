import { Skeleton } from '@components';
import * as React from 'react';
import styled from 'styled-components';

const Row = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const TypeSkeleton = styled(Skeleton.Input)`
    border-radius: 8px;
`;

export default function ContentSectionLoading() {
    return (
        <Row>
            <TypeSkeleton width={60} height={60} />
            <TypeSkeleton width={60} height={60} />
            <TypeSkeleton width={60} height={60} />
            <TypeSkeleton width={60} height={60} />
        </Row>
    );
}
