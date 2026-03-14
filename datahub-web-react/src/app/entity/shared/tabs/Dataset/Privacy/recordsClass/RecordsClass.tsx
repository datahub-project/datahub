import React, { useState } from 'react';
import { Alert, Spin } from 'antd';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { useGetDatasetQuery } from '@graphql/dataset.generated';
import { useGetStructuredPropertyQuery } from '@graphql/structuredProperties.generated';

import { CompliancePropertyQualifiedName } from '@app/entity/shared/tabs/Dataset/Privacy/constant';
import { buildStructuredPropertyUrn } from '@app/entity/shared/tabs/Dataset/Privacy/utils';
import { EditRecordsClass } from '@app/entity/shared/tabs/Dataset/Privacy/recordsClass/EditRecordsClass';
import { ViewRecordsClass } from '@app/entity/shared/tabs/Dataset/Privacy/recordsClass/ViewRecordsClass';

const Container = styled.div`
    padding: 24px;
`;

export function RecordsClass() {
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
        data: recordsClassPropData,
        loading: recordsClassPropLoading,
        error: recordsClassPropError,
    } = useGetStructuredPropertyQuery({
        variables: { urn: buildStructuredPropertyUrn(CompliancePropertyQualifiedName.RecordsClass) },
    });

    const [showEdit, setShowEdit] = useState(false);

    const structuredProps = getDatasetQueryData?.dataset?.structuredProperties?.properties || [];

    const isLoading = getDatasetQueryLoading || recordsClassPropLoading;
    const hasError = getDatasetQueryError || recordsClassPropError;

    if (isLoading) {
        return <Spin tip="Loading Records Class..." size="large" />;
    }

    if (hasError) {
        return <Alert type="error" message="Error loading data" description={getDatasetQueryError?.message} />;
    }

    return (
        <Container>
            {!showEdit && (
                <ViewRecordsClass
                    onEdit={() => setShowEdit(true)}
                    structuredProps={structuredProps}
                />
            )}
            {showEdit && (
                <EditRecordsClass
                    onClose={() => setShowEdit(false)}
                    structuredProps={structuredProps}
                    urn={urn!}
                    refetchDataset={refetchDataset}
                    recordsClassPropData={recordsClassPropData}
                />
            )}
        </Container>
    );
}

export default RecordsClass;
