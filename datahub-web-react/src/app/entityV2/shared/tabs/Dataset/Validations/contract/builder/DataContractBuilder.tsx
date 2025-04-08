import { Button, message } from 'antd';
import lodash from 'lodash';
import React, { useState } from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import {
    createAssertionGroups,
    tryExtractMonitorDetailsFromAssertionsWithMonitorsQuery,
} from '@app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';
import { DataContractAssertionGroupSelect } from '@app/entityV2/shared/tabs/Dataset/Validations/contract/builder/DataContractAssertionGroupSelect';
import {
    DEFAULT_BUILDER_STATE,
    DataContractBuilderState,
    DataContractCategoryType,
} from '@app/entityV2/shared/tabs/Dataset/Validations/contract/builder/types';
import { buildUpsertDataContractMutationVariables } from '@app/entityV2/shared/tabs/Dataset/Validations/contract/builder/utils';
import { DATA_QUALITY_ASSERTION_TYPES } from '@app/entityV2/shared/tabs/Dataset/Validations/contract/utils';
import { useGetDatasetAssertionsWithRunEventsQuery } from '@src/graphql/dataset.generated';

import { useUpsertDataContractMutation } from '@graphql/contract.generated';
import { Assertion, AssertionType, DataContract } from '@types';

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
    const { data: assertionData } = useGetDatasetAssertionsWithRunEventsQuery({
        variables: { urn: entityUrn },
        fetchPolicy: 'cache-first',
    });
    const assertionsWithMonitorsDetails: Assertion[] =
        tryExtractMonitorDetailsFromAssertionsWithMonitorsQuery(assertionData) ?? [];
    const assertionGroups = createAssertionGroups(assertionsWithMonitorsDetails);
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
            .then(({ data, errors }) => {
                if (!errors) {
                    message.success({
                        content: isEdit ? `Edited Data Contract` : `Created Data Contract!`,
                        duration: 3,
                    });
                    onSubmit?.(data?.upsertDataContract as DataContract);
                }
            })
            .catch(() => {
                message.destroy();
                message.error({ content: 'Failed to create Data Contract! An unexpected error occurred' });
            });
    };

    const onSelectFreshnessAssertion = (assertionUrn: string) => {
        const selected = builderState.freshness?.assertionUrn === assertionUrn;
        if (selected) {
            setBuilderState({
                ...builderState,
                freshness: undefined,
            });
        } else {
            setBuilderState({
                ...builderState,
                freshness: { assertionUrn },
            });
        }
    };

    const onSelectSchemaAssertion = (assertionUrn: string) => {
        const selected = builderState.schema?.assertionUrn === assertionUrn;
        if (selected) {
            setBuilderState({
                ...builderState,
                schema: undefined,
            });
        } else {
            setBuilderState({
                ...builderState,
                schema: { assertionUrn },
            });
        }
    };

    const onSelectDataQualityAssertion = (assertionUrn: string) => {
        const selected = builderState.dataQuality?.some((c) => c.assertionUrn === assertionUrn);
        if (selected) {
            setBuilderState({
                ...builderState,
                dataQuality: builderState.dataQuality?.filter((c) => c.assertionUrn !== assertionUrn),
            });
        } else {
            setBuilderState({
                ...builderState,
                dataQuality: [...(builderState.dataQuality || []), { assertionUrn }],
            });
        }
    };

    const editDisabled =
        lodash.isEqual(builderState, initialState) || lodash.isEqual(builderState, DEFAULT_BUILDER_STATE);

    const hasAssertions = freshnessAssertions.length || schemaAssertions.length || dataQualityAssertions.length;

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
                        onSelect={onSelectFreshnessAssertion}
                    />
                )) ||
                    undefined}
                {(schemaAssertions.length && (
                    <DataContractAssertionGroupSelect
                        category={DataContractCategoryType.SCHEMA}
                        assertions={schemaAssertions}
                        multiple={false}
                        selectedUrns={(builderState.schema?.assertionUrn && [builderState.schema?.assertionUrn]) || []}
                        onSelect={onSelectSchemaAssertion}
                    />
                )) ||
                    undefined}
                {(dataQualityAssertions.length && (
                    <DataContractAssertionGroupSelect
                        category={DataContractCategoryType.DATA_QUALITY}
                        assertions={dataQualityAssertions}
                        selectedUrns={builderState.dataQuality?.map((c) => c.assertionUrn) || []}
                        onSelect={onSelectDataQualityAssertion}
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
