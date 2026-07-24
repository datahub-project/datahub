import { Alert, Button, Empty, message } from 'antd';
import lodash from 'lodash';
import React, { useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { Pagination } from '@components/components/Pagination';

import { TableLoadingSkeleton } from '@app/entityV2/shared/TableLoadingSkeleton';
import { createAssertionGroups } from '@app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';
import { DataContractAssertionGroupSelect } from '@app/entityV2/shared/tabs/Dataset/Validations/contract/builder/DataContractAssertionGroupSelect';
import {
    DEFAULT_BUILDER_STATE,
    DataContractBuilderState,
    DataContractCategoryType,
} from '@app/entityV2/shared/tabs/Dataset/Validations/contract/builder/types';
import {
    buildDataContractAssertionSearchInput,
    buildUpsertDataContractMutationVariables,
} from '@app/entityV2/shared/tabs/Dataset/Validations/contract/builder/utils';
import { DATA_QUALITY_ASSERTION_TYPES } from '@app/entityV2/shared/tabs/Dataset/Validations/contract/utils';
import { useSearchAssertionsQuery } from '@src/graphql/assertion.generated';

import { useUpsertDataContractMutation } from '@graphql/contract.generated';
import { Assertion, AssertionType, DataContract } from '@types';

const BuilderContainer = styled.div`
    display: flex;
    flex-direction: column;
    max-height: 70vh;
    height: 70vh;
    overflow: hidden;
`;

const AssertionsSection = styled.div`
    border: 0.5px solid ${(props) => props.theme.colors.bgHover};
    flex: 1;
    overflow: auto;
    min-height: 0;
`;

const HeaderText = styled.div`
    padding: 16px 20px;
    color: ${(props) => props.theme.colors.textTertiary};
    font-size: 16px;
`;

const ActionContainer = styled.div`
    display: flex;
    justify-content: space-between;
    flex-shrink: 0;
    padding: 16px 20px;
    border-top: 1px solid ${(props) => props.theme.colors.bgHover};
    margin-top: 0;
`;

const CancelButton = styled(Button)`
    margin-left: 12px;
`;

const SaveButton = styled(Button)`
    margin-right: 0;
`;

const PaginationContainer = styled.div`
    display: flex;
    justify-content: center;
    padding: 12px 20px;
`;

const ErrorContainer = styled.div`
    padding: 20px;
`;

const ASSERTIONS_PAGE_SIZE = 25;

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
    const { t } = useTranslation('entity.profile.validations');
    const { t: tc } = useTranslation('common.actions');
    const isEdit = !!initialState;
    const [builderState, setBuilderState] = useState(initialState || DEFAULT_BUILDER_STATE);
    const [page, setPage] = useState(1);
    const [upsertDataContractMutation] = useUpsertDataContractMutation();

    // note that for contracts, we do not allow the use of sibling node assertions, for clarity.
    const start = (page - 1) * ASSERTIONS_PAGE_SIZE;
    const {
        data: assertionData,
        previousData,
        loading: assertionsLoading,
        error: assertionsError,
        refetch: refetchAssertions,
    } = useSearchAssertionsQuery({
        variables: {
            input: buildDataContractAssertionSearchInput(entityUrn, start, ASSERTIONS_PAGE_SIZE),
            runEventsLimit: 1,
        },
        fetchPolicy: 'cache-and-network',
    });
    const activeAssertionData = assertionData || previousData;
    const assertions = useMemo(
        () =>
            activeAssertionData?.searchAcrossEntities?.searchResults
                ?.map((result) => result.entity)
                .filter((entity) => entity.__typename === 'Assertion')
                .map((entity) => entity as Assertion) || [],
        [activeAssertionData],
    );
    const totalAssertions = activeAssertionData?.searchAcrossEntities?.total || 0;
    const assertionGroups = createAssertionGroups(assertions);
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
                        content: isEdit ? t('contractBuilder.editedSuccess') : t('contractBuilder.createdSuccess'),
                        duration: 3,
                    });
                    onSubmit?.(data?.upsertDataContract as DataContract);
                }
            })
            .catch(() => {
                message.destroy();
                message.error({ content: t('contractBuilder.failedCreate') });
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
        <BuilderContainer>
            {(hasAssertions && <HeaderText>{t('contractBuilder.selectAssertionsHeader')}</HeaderText>) || (
                <HeaderText>{t('contractBuilder.addAssertionsHeader')}</HeaderText>
            )}
            <AssertionsSection>
                {assertionsLoading && !previousData && <TableLoadingSkeleton />}
                {assertionsError && !activeAssertionData && (
                    <ErrorContainer>
                        <Alert
                            showIcon
                            type="error"
                            message={t('contractBuilder.failedLoadAssertions', {
                                defaultValue: 'Unable to load assertions.',
                            })}
                            action={<Button onClick={() => refetchAssertions()}>{tc('retry')}</Button>}
                        />
                    </ErrorContainer>
                )}
                {!assertionsLoading && !assertionsError && totalAssertions === 0 && (
                    <Empty
                        description={t('contractBuilder.addAssertionsHeader')}
                        image={Empty.PRESENTED_IMAGE_SIMPLE}
                    />
                )}
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
                {totalAssertions > ASSERTIONS_PAGE_SIZE && (
                    <PaginationContainer>
                        <Pagination
                            currentPage={page}
                            itemsPerPage={ASSERTIONS_PAGE_SIZE}
                            total={totalAssertions}
                            onPageChange={setPage}
                            loading={assertionsLoading}
                        />
                    </PaginationContainer>
                )}
            </AssertionsSection>
            <ActionContainer>
                <CancelButton onClick={onCancel}>{tc('cancel')}</CancelButton>
                <div>
                    <SaveButton disabled={editDisabled} type="primary" onClick={upsertDataContract}>
                        {tc('save')}
                    </SaveButton>
                </div>
            </ActionContainer>
        </BuilderContainer>
    );
};
