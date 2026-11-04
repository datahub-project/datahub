import { Skeleton, SkeletonButtonProps } from '@components';
import React from 'react';
import styled from 'styled-components';

const SkeletonContainer = styled.div`
    display: flex;
    flex-direction: column;
`;

const SectionHeader = styled.div`
    display: flex;
    justify-content: space-between;
    margin-bottom: 12px;
`;

const HorizontalList = styled.div`
    display: flex;
    margin-bottom: 12px;
    gap: 12px;
    flex-wrap: nowrap;
    overflow: hidden;
`;

const HeaderSkeleton = styled(Skeleton.Button)`
    border-radius: 12px;
`;

interface Props {
    Component: React.FC<SkeletonButtonProps>;
    showHeader?: boolean;
    count?: number;
}

export const HorizontalListSkeletons = ({ Component, showHeader = true, count = 3 }: Props) => {
    return (
        <SkeletonContainer>
            {showHeader && (
                <SectionHeader>
                    <HeaderSkeleton active size="sm" shape="square" width="10rem" height={24} />
                </SectionHeader>
            )}
            <HorizontalList>
                {Array.from({ length: count }, (_, index) => (
                    <Component active shape="square" block key={index} />
                ))}
            </HorizontalList>
        </SkeletonContainer>
    );
};
