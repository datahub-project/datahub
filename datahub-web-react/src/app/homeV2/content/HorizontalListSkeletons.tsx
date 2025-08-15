import { Skeleton } from 'antd';
import { SkeletonButtonProps } from 'antd/lib/skeleton/Button';
import React from 'react';
import styled from 'styled-components';

const SkeletonContainer = styled.div`
    && {
        display: flex;
        flex-direction: column;

        .ant-skeleton-button-sm {
            border-radius: 4px;
        }
    }
`;

const SectionHeader = styled.div`
    display: flex;
    justify-content: space-between;
    margin-bottom: 12px;

    .ant-skeleton {
        width: auto;
    }

    .ant-skeleton-button {
        height: 100%;
        border-radius: 12px;
        background-color: #f8f9fa;
    }
`;

const HorizontalList = styled.div`
    display: flex;
    margin-bottom: 12px;
    gap: 12px;
    flex-wrap: nowrap;
    overflow: hidden;
`;

const HeaderSkeleton = styled(Skeleton.Button)<{ width?: string }>`
    &&& {
        height: 24px;
        width: ${(props) => props?.width};
    }
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
                    <HeaderSkeleton active size="small" shape="square" block width="10rem" />
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
