import React, { useMemo } from 'react';

import CellSkeleton from '@app/entityV2/shared/tabs/Dataset/Schema/components/CellSkeleton';
import StructuredPropValues from '@src/app/entityV2/dataset/profile/schema/components/StructuredPropValues';
import { getDisplayName } from '@src/app/govern/structuredProperties/utils';
import { SearchResult, StructuredPropertyEntity } from '@src/types.generated';

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
