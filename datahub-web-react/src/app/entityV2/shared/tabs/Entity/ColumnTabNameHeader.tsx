import React from 'react';
import { useGetEntityWithSchema } from '../Dataset/Schema/useGetEntitySchema';
import TabNameWithCount from './TabNameWithCount';

const ColumnTabNameHeader = () => {
    const { entityWithSchema, loading } = useGetEntityWithSchema();
    const fieldsCount = entityWithSchema?.schemaMetadata?.fields?.length || 0;

    return <TabNameWithCount name="Columns" count={fieldsCount} loading={loading} />;
};

export default ColumnTabNameHeader;
