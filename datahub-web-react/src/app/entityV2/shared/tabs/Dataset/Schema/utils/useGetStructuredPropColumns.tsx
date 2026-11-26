import React, { useMemo } from 'react';

import { Column } from '@components/components/Table/types';

import { ExtendedSchemaFields } from '@app/entityV2/dataset/profile/schema/utils/types';
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
            const column: Column<ExtendedSchemaFields> = {
                width: '150px',
                title: name,
                key: prop.entity.urn,
                render: (record) =>
                    renderMetadataCell(
                        metadataStatus,
                        150,
                        () => (
                            <StructuredPropValues
                                schemaFieldEntity={record.schemaFieldEntity ?? undefined}
                                propColumn={prop}
                            />
                        ),
                        'prop-cell-skeleton',
                    ),
            };
            return column;
        });
    }, [properties, metadataStatus]);

    return columns;
};
