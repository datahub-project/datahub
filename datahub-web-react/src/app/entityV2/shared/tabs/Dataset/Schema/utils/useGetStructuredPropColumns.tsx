import React, { useMemo } from 'react';
import styled from 'styled-components';

import StructuredPropValues from '@src/app/entityV2/dataset/profile/schema/components/StructuredPropValues';
import { getDisplayName } from '@src/app/govern/structuredProperties/utils';
import { SearchResult, StructuredPropertyEntity } from '@src/types.generated';

// Neutral shimmer block standing in for a cell value while the full-metadata query is still
// in flight (master's no-antd-imports lint rule rules out antd's Skeleton for new files).
const CellSkeleton = styled.div`
    width: 80px;
    height: 20px;
    border-radius: 4px;
    background: linear-gradient(
        90deg,
        ${(props) => props.theme.colors.bgSkeleton} 25%,
        ${(props) => props.theme.colors.bgSkeletonShimmer} 37%,
        ${(props) => props.theme.colors.bgSkeleton} 63%
    );
    background-size: 400% 100%;
    animation: cell-skeleton-shimmer 1.4s ease infinite;
    @keyframes cell-skeleton-shimmer {
        0% {
            background-position: 100% 50%;
        }
        100% {
            background-position: 0 50%;
        }
    }
`;

export const useGetStructuredPropColumns = (properties: SearchResult[] | undefined, fullMetadataLoading?: boolean) => {
    const columns = useMemo(() => {
        return properties?.map((prop) => {
            const name = getDisplayName(prop.entity as StructuredPropertyEntity);
            return {
                width: 120,
                title: name,
                dataIndex: 'schemaFieldEntity',
                key: prop.entity.urn,
                render: (record) =>
                    fullMetadataLoading ? (
                        <CellSkeleton data-testid="prop-cell-skeleton" />
                    ) : (
                        <StructuredPropValues schemaFieldEntity={record} propColumn={prop} />
                    ),
                ellipsis: true,
            };
        });
    }, [properties, fullMetadataLoading]);

    return columns;
};
