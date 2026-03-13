import React, { useState } from 'react';
import { Alert, Spin } from 'antd';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { useGetDatasetQuery, useGetDatasetSchemaQuery } from '@graphql/dataset.generated';
import { useGetMeQuery } from '@graphql/me.generated';
import { useGetStructuredPropertyQuery } from '@graphql/structuredProperties.generated';

import { CompliancePropertyQualifiedName } from '@app/entity/shared/tabs/Dataset/Privacy/constant';
import { buildStructuredPropertyUrn } from '@app/entity/shared/tabs/Dataset/Privacy/utils';
import { EditAnnotation } from '@app/entity/shared/tabs/Dataset/Privacy/pii/EditAnnotation';
import { ViewAnnotation } from '@app/entity/shared/tabs/Dataset/Privacy/pii/ViewAnnotation';

const Container = styled.div`
    padding: 24px;
`;

export function Annotation() {
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

    const { data, loading, error, refetch } = useGetDatasetSchemaQuery({
        variables: { urn: urn! },
        skip: !urn,
        fetchPolicy: 'network-only',
    });

    const { data: getMeData, loading: getMeLoading, error: getMeError } = useGetMeQuery();

    const {
        data: piiAnnotationPropData,
        loading: piiAnnotationPropLoading,
        error: piiAnnotationPropError,
    } = useGetStructuredPropertyQuery({
        variables: { urn: buildStructuredPropertyUrn(CompliancePropertyQualifiedName.PiiAnnotation) },
    });

    const {
        data: hasPersonalDataPropData,
        loading: hasPersonalDataPropLoading,
        error: hasPersonalDataPropError,
    } = useGetStructuredPropertyQuery({
        variables: { urn: buildStructuredPropertyUrn(CompliancePropertyQualifiedName.HasPersonalData) },
    });

    const [showEdit, setShowEdit] = useState(false);

    const structuredProps = getDatasetQueryData?.dataset?.structuredProperties?.properties || [];
    const fields = data?.dataset?.schemaMetadata?.fields || [];

    const isLoading = loading || getDatasetQueryLoading || getMeLoading || piiAnnotationPropLoading || hasPersonalDataPropLoading;
    const hasError = error || getDatasetQueryError || getMeError || piiAnnotationPropError || hasPersonalDataPropError;

    if (isLoading) {
        return <Spin tip="Loading PII Annotations..." size="large" />;
    }

    if (hasError) {
        return <Alert type="error" message="Error loading data" description={error?.message} />;
    }

    if (!fields.length) {
        return <Alert type="info" message="No columns found in schema" />;
    }

    return (
        <Container>
            {!showEdit && (
                <ViewAnnotation
                    onEdit={() => setShowEdit(true)}
                    fields={fields}
                    structuredProps={structuredProps}
                />
            )}
            {showEdit && (
                <EditAnnotation
                    onClose={() => setShowEdit(false)}
                    fields={fields}
                    structuredProps={structuredProps}
                    urn={urn!}
                    refetch={refetch}
                    refetchDataset={refetchDataset}
                    getMeData={getMeData}
                    piiAnnotationPropData={piiAnnotationPropData}
                    hasPersonalDataPropData={hasPersonalDataPropData}
                />
            )}
        </Container>
    );
}

export default Annotation;
