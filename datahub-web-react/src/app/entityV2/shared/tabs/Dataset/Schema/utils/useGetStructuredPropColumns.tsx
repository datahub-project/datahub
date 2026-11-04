import React, { useMemo } from 'react';

import { MetadataStatus, renderMetadataCell } from '@app/entityV2/shared/tabs/Dataset/Schema/metadataStatus';
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
                width: 150,
                title: name,
                dataIndex: 'schemaFieldEntity',
                key: prop.entity.urn,
                render: (record) =>
                    renderMetadataCell(
                        metadataStatus,
                        150,
                        () => <StructuredPropValues schemaFieldEntity={record} propColumn={prop} />,
                        'prop-cell-skeleton',
                    ),
                ellipsis: true,
            };
        });
    }, [properties, metadataStatus]);

    return columns;
};
