import React, { useState } from 'react';
import { message, Button } from 'antd';
import styled from 'styled-components';
import lodash from 'lodash';
import { DataContract, AssertionType, Assertion } from '../../../../../../../../types.generated';
import { DataContractBuilderState, DataContractCategoryType, DEFAULT_BUILDER_STATE } from './types';
import { buildUpsertDataContractMutationVariables } from './utils';
import { useUpsertDataContractMutation } from '../../../../../../../../graphql/contract.generated';
import { createAssertionGroups } from '../../utils';
import { DataContractAssertionGroupSelect } from './DataContractAssertionGroupSelect';
import { ANTD_GRAY } from '../../../../../constants';
import { DATA_QUALITY_ASSERTION_TYPES } from '../utils';
import { useGetDatasetAssertionsQuery } from '../../../../../../../../graphql/dataset.generated';

const AssertionsSection = styled.div`
    border: 0.5px solid ${ANTD_GRAY[4]};
`;

const HeaderText = styled.div`
    padding: 16px 20px;
    color: ${ANTD_GRAY[7]};
    font-size: 16px;
`;

const ActionContainer = styled.div`
    display: flex;
    justify-content: space-between;
    margin-top: 16px;
`;

const CancelButton = styled(Button)`
    margin-left: 12px;
`;

const SaveButton = styled(Button)`
    margin-right: 20px;
`;

type Props = {
    entityUrn: string;
    initialState?: DataContractBuilderState;
    onSubmit?: (contract: DataContract) => void;
    onCancel?: () => void;
};

/**
 * This component is a modal used for constructing new Data Contracts
 *
 * In order to build a data contract, we simply list all dataset assertions and allow the user to choose.
 */
export const DataContractBuilder = ({ entityUrn, initialState, onSubmit, onCancel }: Props) => {
    const isEdit = !!initialState;
    const [builderState, setBuilderState] = useState(initialState || DEFAULT_BUILDER_STATE);
    const [upsertDataContractMutation] = useUpsertDataContractMutation();

    // note that for contracts, we do not allow the use of sibling node assertions, for clarity.
    const { data } = useGetDatasetAssertionsQuery({
        variables: { urn: entityUrn },
        fetchPolicy: 'cache-first',
    });
    const assertionData = data?.dataset?.assertions?.assertions ?? [];

    const assertionGroups = createAssertionGroups(assertionData as Array<Assertion>);
    const freshnessAssertions =
        assertionGroups.find((group) => group.type === AssertionType.Freshness)?.assertions || [];
    const schemaAssertions = assertionGroups.find((group) => group.type === AssertionType.DataSchema)?.assertions || [];
    const dataQualityAssertions = assertionGroups
        .filter((group) => DATA_QUALITY_ASSERTION_TYPES.has(group.type))
        .flatMap((group) => group.assertions || []);

    /**
     * Upserts the Data Contract for an entity
     */
    const upsertDataContract = () => {
        return upsertDataContractMutation({
            variables: buildUpsertDataContractMutationVariables(entityUrn, builderState),
        })
            .then(({ data: dataContract, errors }) => {
                if (!errors) {
                    message.success({
                        content: isEdit ? `Edited Data Contract` : `Created Data Contract!`,
                        duration: 3,
                    });
                    onSubmit?.(dataContract?.upsertDataContract as DataContract);
                }
            })
            .catch(() => {
                message.destroy();
                message.error({ content: 'Failed to create Data Contract! An unexpected error occurred' });
            });
    };

    const onSelectDataAssertion = (assertionUrn: string, type: string) => {
        const selected = builderState[type]?.some((c) => c.assertionUrn === assertionUrn);
        if (selected) {
            setBuilderState({
                ...builderState,
                [type]: builderState[type]?.filter((c) => c.assertionUrn !== assertionUrn),
            });
        } else {
            setBuilderState({
                ...builderState,
                [type]: [...(builderState[type] || []), { assertionUrn }],
            });
        }
    };

    const editDisabled =
        lodash.isEqual(builderState, initialState) || lodash.isEqual(builderState, DEFAULT_BUILDER_STATE);

    const hasAssertions = freshnessAssertions.length || schemaAssertions.length || dataQualityAssertions.length;

    const onSelectFreshnessOrSchemaAssertion = (assertionUrn: string, type: string) => {
        const selected = builderState[type]?.assertionUrn === assertionUrn;
        if (selected) {
            setBuilderState({
                ...builderState,
                [type]: undefined,
            });
        } else {
            setBuilderState({
                ...builderState,
                [type]: { assertionUrn },
            });
        }
    };

    return (
        <>
            {(hasAssertions && <HeaderText>Select the assertions that will make up your contract.</HeaderText>) || (
                <HeaderText>Add a few assertions on this entity to create a data contract out of them.</HeaderText>
            )}
            <AssertionsSection>
                {(freshnessAssertions.length && (
                    <DataContractAssertionGroupSelect
                        category={DataContractCategoryType.FRESHNESS}
                        assertions={freshnessAssertions}
                        multiple={false}
                        selectedUrns={
                            (builderState.freshness?.assertionUrn && [builderState.freshness?.assertionUrn]) || []
                        }
                        onSelect={(selectedUrn: string) => onSelectFreshnessOrSchemaAssertion(selectedUrn, 'freshness')}
                    />
                )) ||
                    undefined}
                {(schemaAssertions.length && (
                    <DataContractAssertionGroupSelect
                        category={DataContractCategoryType.SCHEMA}
                        assertions={schemaAssertions}
                        multiple={false}
                        selectedUrns={(builderState.schema?.assertionUrn && [builderState.schema?.assertionUrn]) || []}
                        onSelect={(selectedUrn: string) => onSelectFreshnessOrSchemaAssertion(selectedUrn, 'schema')}
                    />
                )) ||
                    undefined}
                {(dataQualityAssertions.length && (
                    <DataContractAssertionGroupSelect
                        category={DataContractCategoryType.DATA_QUALITY}
                        assertions={dataQualityAssertions}
                        selectedUrns={builderState.dataQuality?.map((c) => c.assertionUrn) || []}
                        onSelect={(selectedUrn: string) => onSelectDataAssertion(selectedUrn, 'dataQuality')}
                    />
                )) ||
                    undefined}
            </AssertionsSection>
            <ActionContainer>
                <CancelButton onClick={onCancel}>Cancel</CancelButton>
                <div>
                    <SaveButton disabled={editDisabled} type="primary" onClick={upsertDataContract}>
                        Save
                    </SaveButton>
                </div>
            </ActionContainer>
        </>
    );
};
