import { Checkbox, EmptyState, Table } from '@components';
import { ShieldCheck } from '@phosphor-icons/react/dist/csr/ShieldCheck';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import type { Column } from '@components/components/Table/types';

import { useEntityData } from '@app/entityV2/shared/EntityContext';
import {
    ActionsColumn,
    DetailsColumn,
} from '@app/entityV2/shared/tabs/Dataset/Validations/AcrylAssertionsTableColumns';
import { getEntityUrnForAssertion, getSiblingWithUrn } from '@app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';
import { useOpenAssertionDetailModal } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/hooks';
import { AssertionProfileDrawer } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/AssertionProfileDrawer';

import { Assertion, AssertionRunStatus, DataContract } from '@types';

const DetailsColumnWrapper = styled.div`
    display: flex;
    align-items: center;
    gap: 12px;
`;

const EmptyContainer = styled.div`
    padding: 48px 0;
`;

type Props = {
    assertions: Array<Assertion>;
    contract?: DataContract;
    showMenu?: boolean;
    showSelect?: boolean;
    selectedUrns?: string[];
    onSelect?: (assertionUrn: string) => void;
    refetch?: () => void;
};

/**
 * Acryl-specific list of assertions displaying their most recent run status, their human-readable
 * description, and platform.
 */
export const AcrylAssertionsTable = ({
    assertions,
    contract,
    showMenu = true,
    showSelect = false,
    selectedUrns,
    onSelect,
    refetch,
}: Props) => {
    const { t } = useTranslation('entity.profile.validations');
    const { entityData } = useEntityData();
    const [focusAssertionUrn, setFocusAssertionUrn] = useState<string | null>(null);

    const focusedAssertion = assertions.find((assertion) => assertion.urn === focusAssertionUrn);
    const focusedEntityUrn = focusedAssertion ? getEntityUrnForAssertion(focusedAssertion) : undefined;
    const focusedAssertionEntity =
        focusedEntityUrn && entityData ? getSiblingWithUrn(entityData, focusedEntityUrn) : undefined;

    if (focusAssertionUrn && !focusedAssertion) {
        setFocusAssertionUrn(null);
    }

    useOpenAssertionDetailModal(setFocusAssertionUrn);

    const assertionsTableData = assertions.map((assertion) => ({
        urn: assertion.urn,
        type: assertion.info?.type,
        platform: assertion.platform,
        datasetAssertionInfo: assertion.info?.datasetAssertion,
        freshnessAssertionInfo: assertion.info?.freshnessAssertion,
        lastEvaluation:
            assertion.runEvents?.runEvents?.length &&
            assertion.runEvents.runEvents[0].status === AssertionRunStatus.Complete &&
            assertion.runEvents.runEvents[0],
        lastEvaluationTimeMs:
            assertion.runEvents?.runEvents?.length && assertion.runEvents.runEvents[0].timestampMillis,
        lastEvaluationResult:
            assertion.runEvents?.runEvents?.length &&
            assertion.runEvents.runEvents[0].status === AssertionRunStatus.Complete &&
            assertion.runEvents.runEvents[0].result?.type,
        lastEvaluationUrl:
            assertion.runEvents?.runEvents?.length &&
            assertion.runEvents.runEvents[0].status === AssertionRunStatus.Complete &&
            assertion.runEvents.runEvents[0].result?.externalUrl,
        assertion,
    }));

    type AssertionRow = (typeof assertionsTableData)[number];

    const assertionsTableCols: Column<AssertionRow>[] = [
        {
            title: '',
            key: 'details',
            render: (record) => {
                const selected = selectedUrns?.some((selectedUrn) => selectedUrn === record.urn);
                return (
                    <DetailsColumnWrapper>
                        {showSelect && (
                            <Checkbox
                                isChecked={selected}
                                onCheckboxChange={() => onSelect?.(record.urn)}
                                aria-label={record.urn}
                            />
                        )}
                        <DetailsColumn
                            assertion={record.assertion}
                            contract={contract}
                            lastEvaluation={record.lastEvaluation || undefined}
                            onViewAssertionDetails={() => setFocusAssertionUrn(record.urn)}
                        />
                    </DetailsColumnWrapper>
                );
            },
        },
        {
            title: '',
            key: 'actions',
            alignment: 'right',
            // TODO: Add permission for editing contract.
            render: (record) =>
                showMenu ? (
                    <ActionsColumn assertion={record.assertion} contract={contract} canEditContract refetch={refetch} />
                ) : null,
        },
    ];

    return (
        <>
            {assertionsTableData.length === 0 ? (
                <EmptyContainer>
                    <EmptyState icon={ShieldCheck} title={t('assertionList.noAssertionsTitle')} size="sm" />
                </EmptyContainer>
            ) : (
                <Table<AssertionRow>
                    columns={assertionsTableCols}
                    data={assertionsTableData}
                    rowKey="urn"
                    showHeader={false}
                    focusedRowKey={focusAssertionUrn}
                    onRowClick={(record) => {
                        if (showSelect) {
                            onSelect?.(record.urn);
                        } else {
                            setFocusAssertionUrn(record.urn);
                        }
                    }}
                />
            )}
            {focusAssertionUrn && focusedAssertionEntity && (
                <AssertionProfileDrawer
                    urn={focusAssertionUrn}
                    contract={contract}
                    closeDrawer={() => setFocusAssertionUrn(null)}
                    refetch={refetch}
                />
            )}
        </>
    );
};
