import React, { useState } from 'react';
import styled from 'styled-components';
import { useGetDatasetContractQuery } from '../../../../../../../graphql/contract.generated';
import { DataContract, DataContractState } from '../../../../../../../types.generated';
import { useEntityData } from '../../../../../../entity/shared/EntityContext';
import { DataContractEmptyState } from '../../../../../../entity/shared/tabs/Dataset/Validations/contract/DataContractEmptyState';
import { getAssertionsSummary } from '../acrylUtils';
import { ContractStructuredPropertiesSummary } from './ContractStructuredPropertiesSummary';
import { DataContractSummary } from './DataContractSummary';
import { DataQualityContractSummary } from './DataQualityContractSummary';
import { FreshnessContractSummary } from './FreshnessContractSummary';
import { SchemaContractSummary } from './SchemaContractSummary';
import { DataContractBuilderModal } from './builder/DataContractBuilderModal';

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
    const hasStructuredProperties =
        contract?.structuredProperties && (contract?.structuredProperties?.properties?.length || 0) > 0;
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
                            {hasStructuredProperties && contract && (
                                <ContractStructuredPropertiesSummary
                                    contract={contract as DataContract}
                                    refetch={refetch}
                                />
                            )}
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
                    entityUrn={urn}
                    onCancel={() => setShowContractBuilder(false)}
                    onSubmit={onContractUpdate}
                />
            )}
        </>
    );
};
