import React, { useState } from 'react';
import styled from 'styled-components';
import { useGetDatasetContractQuery } from '../../../../../../../graphql/contract.generated';
import { DataContractState } from '../../../../../../../types.generated';
import { useEntityData } from '../../../../EntityContext';
import { DataContractEmptyState } from './DataContractEmptyState';
import { DataContractSummary } from './DataContractSummary';
import { DataQualityContractSummary } from './DataQualityContractSummary';
import { SchemaContractSummary } from './SchemaContractSummary';
import { FreshnessContractSummary } from './FreshnessContractSummary';
import { DataContractBuilderModal } from './builder/DataContractBuilderModal';
import { createBuilderState } from './builder/utils';
import { getAssertionsSummary } from '../utils';

const Container = styled.div`
    display: flex;
`;

const LeftColumn = styled.div`
    width: 50%;
`;

const RightColumn = styled.div`
    width: 50%;
`;

/**
 * Component used for rendering the Data Contract Tab on the Assertions parent tab.
 */
export const DataContractTab = () => {
    const { urn } = useEntityData();

    const { data, refetch } = useGetDatasetContractQuery({
        variables: {
            urn,
        },
    });
    const [showContractBuilder, setShowContractBuilder] = useState(false);

    const contract = data?.dataset?.contract;
    const schemaContracts = data?.dataset?.contract?.properties?.schema || [];
    const freshnessContracts = data?.dataset?.contract?.properties?.freshness || [];
    const dataQualityContracts = data?.dataset?.contract?.properties?.dataQuality || [];
    const schemaAssertions = schemaContracts.map((c) => c.assertion);
    const freshnessAssertions = freshnessContracts.map((c) => c.assertion);
    const dataQualityAssertions = dataQualityContracts.map((c) => c.assertion);
    const assertionsSummary = getAssertionsSummary([
        ...schemaAssertions,
        ...freshnessAssertions,
        ...dataQualityAssertions,
    ] as any);
    const contractState = data?.dataset?.contract?.status?.state || DataContractState.Active;
    const hasFreshnessContract = freshnessContracts && freshnessContracts?.length;
    const hasSchemaContract = schemaContracts && schemaContracts?.length;
    const hasDataQualityContract = dataQualityContracts && dataQualityContracts?.length;
    const showLeftColumn = hasFreshnessContract || hasSchemaContract || undefined;

    const onContractUpdate = () => {
        if (contract) {
            // Contract exists, just refetch.
            refetch();
        } else {
            // no contract yet, wait for indxing,
            setTimeout(() => refetch(), 3000);
        }
        setShowContractBuilder(false);
    };

    return (
        <>
            {data?.dataset?.contract ? (
                <>
                    <DataContractSummary
                        state={contractState}
                        summary={assertionsSummary}
                        showContractBuilder={() => setShowContractBuilder(true)}
                    />
                    <Container>
                        {showLeftColumn && (
                            <LeftColumn>
                                {(hasFreshnessContract && (
                                    <FreshnessContractSummary
                                        contracts={freshnessContracts as any}
                                        showAction={false}
                                    />
                                )) ||
                                    undefined}
                                {(hasSchemaContract && (
                                    <SchemaContractSummary contracts={schemaContracts as any} showAction={false} />
                                )) ||
                                    undefined}
                            </LeftColumn>
                        )}
                        <RightColumn>
                            {hasDataQualityContract ? (
                                <DataQualityContractSummary
                                    contracts={dataQualityContracts as any}
                                    showAction={false}
                                />
                            ) : undefined}
                        </RightColumn>
                    </Container>
                </>
            ) : (
                <DataContractEmptyState showContractBuilder={() => setShowContractBuilder(true)} />
            )}
            {showContractBuilder && (
                <DataContractBuilderModal
                    initialState={createBuilderState(data?.dataset?.contract as any)}
                    entityUrn={urn}
                    onCancel={() => setShowContractBuilder(false)}
                    onSubmit={onContractUpdate}
                />
            )}
        </>
    );
};
