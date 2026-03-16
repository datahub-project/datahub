import React, { useState } from 'react';
import { Alert, Spin } from 'antd';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { useGetDatasetQuery } from '@graphql/dataset.generated';
import { useGetStructuredPropertyQuery } from '@graphql/structuredProperties.generated';

import { CompliancePropertyQualifiedName } from '@app/entity/shared/tabs/Dataset/Privacy/constant';
import { buildStructuredPropertyUrn } from '@app/entity/shared/tabs/Dataset/Privacy/utils';
import { EditScrubbing } from '@app/entity/shared/tabs/Dataset/Privacy/scrubbing/EditScrubbing';
import { ViewScrubbing } from '@app/entity/shared/tabs/Dataset/Privacy/scrubbing/ViewScrubbing';

const Container = styled.div`
    padding: 24px;
`;

export function Scrubbing() {
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
        data: scrubbingOpPropData,
        loading: scrubbingOpPropLoading,
        error: scrubbingOpPropError,
    } = useGetStructuredPropertyQuery({
        variables: { urn: buildStructuredPropertyUrn(CompliancePropertyQualifiedName.ScrubbingOp) },
    });

    const {
        data: scrubbingStatePropData,
        loading: scrubbingStatePropLoading,
        error: scrubbingStatePropError,
    } = useGetStructuredPropertyQuery({
        variables: { urn: buildStructuredPropertyUrn(CompliancePropertyQualifiedName.ScrubbingState) },
    });

    const {
        data: scrubbingStatusPropData,
        loading: scrubbingStatusPropLoading,
        error: scrubbingStatusPropError,
    } = useGetStructuredPropertyQuery({
        variables: { urn: buildStructuredPropertyUrn(CompliancePropertyQualifiedName.ScrubbingStatus) },
    });

    const [showEdit, setShowEdit] = useState(false);

    const structuredProps = getDatasetQueryData?.dataset?.structuredProperties?.properties || [];

    const isLoading =
        getDatasetQueryLoading || scrubbingOpPropLoading || scrubbingStatePropLoading || scrubbingStatusPropLoading;
    const hasError =
        getDatasetQueryError || scrubbingOpPropError || scrubbingStatePropError || scrubbingStatusPropError;

    if (isLoading) {
        return <Spin tip="Loading Scrubbing..." size="large" />;
    }

    if (hasError) {
        return <Alert type="error" message="Error loading data" description={getDatasetQueryError?.message} />;
    }

    return (
        <Container>
            {!showEdit && <ViewScrubbing onEdit={() => setShowEdit(true)} structuredProps={structuredProps} />}
            {showEdit && (
                <EditScrubbing
                    onClose={() => setShowEdit(false)}
                    structuredProps={structuredProps}
                    urn={urn!}
                    refetchDataset={refetchDataset}
                    scrubbingOpPropData={scrubbingOpPropData}
                    scrubbingStatePropData={scrubbingStatePropData}
                    scrubbingStatusPropData={scrubbingStatusPropData}
                />
            )}
        </Container>
    );
}

export default Scrubbing;