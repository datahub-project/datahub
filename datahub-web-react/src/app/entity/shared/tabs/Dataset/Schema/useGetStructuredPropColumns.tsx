import StructuredPropValues from '@src/app/entity/dataset/profile/schema/components/StructuredPropValues';
import { getDisplayName } from '@src/app/govern/structuredProperties/utils';
import { SearchResult, StructuredPropertyEntity } from '@src/types.generated';
import React, { useMemo } from 'react';

export const useGetStructuredPropColumns = (properties: SearchResult[] | undefined) => {
    const columns = useMemo(() => {
        return properties?.map((prop) => {
            const name = getDisplayName(prop.entity as StructuredPropertyEntity);
            return {
                width: 120,
                title: name,
                dataIndex: 'schemaFieldEntity',
                key: prop.entity.urn,
                render: (record) => <StructuredPropValues schemaFieldEntity={record} propColumn={prop} />,
                ellipsis: true,
            };
        });
    }, [properties]);

    return columns;
};
