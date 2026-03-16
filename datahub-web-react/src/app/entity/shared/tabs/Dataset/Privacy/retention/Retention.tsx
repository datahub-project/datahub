import { Alert, Spin } from 'antd';
import React, { useMemo, useState } from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { EditRetention } from '@app/entity/shared/tabs/Dataset/Privacy/retention/EditRetention';
import { ViewRetention } from '@app/entity/shared/tabs/Dataset/Privacy/retention/ViewRetention';
import { SchemaFieldDataType } from '@src/types.generated';

import { useGetDatasetQuery, useGetDatasetSchemaQuery } from '@graphql/dataset.generated';

const Container = styled.div`
    padding: 24px;
`;

export function Retention() {
    const { urn } = useEntityData();

    const {
        data: getDatasetQueryData,
        loading: getDatasetQueryLoading,
        error: getDatasetQueryError,
        refetch: refetchDataset,
    } = useGetDatasetQuery({
        variables: { urn: urn! },
        skip: !urn,
        fetchPolicy: 'network-only',
    });

    const {
        data: schemaData,
        loading: schemaLoading,
        error: schemaError,
    } = useGetDatasetSchemaQuery({
        variables: { urn: urn! },
        skip: !urn,
        fetchPolicy: 'network-only',
    });

    const dateFieldNames = useMemo(() => {
        const fields = schemaData?.dataset?.schemaMetadata?.fields ?? [];
        const DATE_TYPES: SchemaFieldDataType[] = [SchemaFieldDataType.Date, SchemaFieldDataType.Time];
        return fields.filter((f) => DATE_TYPES.includes(f.type)).map((f) => f.fieldPath);
    }, [schemaData]);

    const [showEdit, setShowEdit] = useState(false);

    const structuredProps = getDatasetQueryData?.dataset?.structuredProperties?.properties || [];

    const isLoading = getDatasetQueryLoading || schemaLoading;
    const hasError = getDatasetQueryError || schemaError;

    if (isLoading) {
        return <Spin tip="Loading Retention..." size="large" />;
    }

    if (hasError) {
        return <Alert type="error" message="Error loading data" description={getDatasetQueryError?.message} />;
    }

    return (
        <Container>
            {!showEdit && <ViewRetention onEdit={() => setShowEdit(true)} structuredProps={structuredProps} />}
            {showEdit && (
                <EditRetention
                    onClose={() => setShowEdit(false)}
                    structuredProps={structuredProps}
                    urn={urn!}
                    refetchDataset={refetchDataset}
                    dateFieldNames={dateFieldNames}
                />
            )}
        </Container>
    );
}

export default Retention;
