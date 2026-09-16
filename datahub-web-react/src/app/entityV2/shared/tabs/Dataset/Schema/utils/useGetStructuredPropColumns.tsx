import React, { useMemo } from 'react';

import CellSkeleton from '@app/entityV2/shared/tabs/Dataset/Schema/components/CellSkeleton';
import MetadataUnavailable from '@app/entityV2/shared/tabs/Dataset/Schema/components/MetadataUnavailable';
import { MetadataStatus } from '@app/entityV2/shared/tabs/Dataset/Schema/metadataStatus';
import StructuredPropValues from '@src/app/entityV2/dataset/profile/schema/components/StructuredPropValues';
import { getDisplayName } from '@src/app/govern/structuredProperties/utils';
import { SearchResult, StructuredPropertyEntity } from '@src/types.generated';

export const useGetStructuredPropColumns = (
    properties: SearchResult[] | undefined,
    metadataStatus: MetadataStatus = 'ready',
) => {
    const columns = useMemo(() => {
        return properties?.map((prop) => {
            const name = getDisplayName(prop.entity as StructuredPropertyEntity);
            return {
                width: 120,
                title: name,
                dataIndex: 'schemaFieldEntity',
                key: prop.entity.urn,
                render: (record) => {
                    if (metadataStatus === 'loading')
                        return <CellSkeleton $width={120} data-testid="prop-cell-skeleton" />;
                    if (metadataStatus === 'error') return <MetadataUnavailable />;
                    return <StructuredPropValues schemaFieldEntity={record} propColumn={prop} />;
                },
                ellipsis: true,
            };
        });
    }, [properties, metadataStatus]);

    return columns;
};
