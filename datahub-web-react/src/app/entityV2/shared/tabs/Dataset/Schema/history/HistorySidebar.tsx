import CloseOutlinedIcon from '@mui/icons-material/CloseOutlined';
import { Drawer } from 'antd';
import React, { useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';

import { SimpleSelect } from '@components/components/Select/SimpleSelect';

import { useGetSiblingPlatforms } from '@app/entity/shared/siblingUtils';
import ChangeTransactionView, {
    ChangeTransactionEntry,
} from '@app/entityV2/shared/tabs/Dataset/Schema/history/ChangeTransactionView';
import { getCategoryOptions } from '@app/entityV2/shared/tabs/Dataset/Schema/history/HistorySidebar.utils';
import { useResolveEntityNames } from '@app/entityV2/shared/tabs/Dataset/Schema/history/useResolveEntityNames';

import { useGetTimelineQuery } from '@graphql/timeline.generated';
import { ChangeCategoryType, ChangeTransaction, DataPlatform, EntityType, SemanticVersionStruct } from '@types';

const MAX_CHANGE_TRANSACTIONS = 100;

const StyledDrawer = styled(Drawer)`
    &&& .ant-drawer-body {
        padding: 0;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        height: 100%;
        overflow-x: hidden;
    }

    &&& .ant-drawer-content-wrapper {
        box-shadow: -20px 0px 44px 0px ${(props) => props.theme.colors.overlayMedium};
    }
`;

const DrawerContent = styled.div`
    height: 100%;
`;

const FieldHeaderWrapper = styled.div`
    padding: 16px;
    display: flex;
    justify-content: space-between;
    align-items: center;
    background: ${(p) => p.theme.styles['primary-color']};
    color: ${(props) => props.theme.colors.textOnFillBrand};
    font-size: 14px;
    font-weight: 700;
`;

const ChangeTransactionList = styled.div`
    display: flex;
    flex-direction: column;
    padding: 26px;
`;

const CloseIcon = styled.div`
    display: flex;
    &&:hover {
        cursor: pointer;
        stroke: ${(props) => props.theme.colors.textOnFillBrand};
    }
`;

const HistoryFooter = styled.div`
    padding: 12px 26px;
    text-align: center;
    font-size: 12px;
    color: ${(props) => props.theme.colors.textTertiary};
    border-top: 1px solid ${(props) => props.theme.colors.border};
`;

interface Props {
    open: boolean;
    onClose: () => void;
    urn: string;
    siblingUrn?: string;
    versionList: SemanticVersionStruct[];
    hideSemanticVersions: boolean;
    entityType?: EntityType;
}

const HistorySidebar = ({ open, onClose, urn, siblingUrn, versionList, hideSemanticVersions, entityType }: Props) => {
    const categoryOptions = useMemo(() => getCategoryOptions(entityType), [entityType]);
    const allCategoryValues = useMemo(() => categoryOptions.map((o) => o.value), [categoryOptions]);
    const [selectedCategories, setSelectedCategories] = useState<string[]>(allCategoryValues);

    // Reset selection when the available categories change (e.g. navigating between entity types)
    useEffect(() => setSelectedCategories(allCategoryValues), [allCategoryValues]);

    const { data: entityTimelineData, error: entityTimelineError } = useGetTimelineQuery({
        skip: !open,
        variables: {
            input: {
                urn,
                changeCategories: allCategoryValues as ChangeCategoryType[],
            },
        },
    });
    const { data: siblingTimelineData, error: siblingTimelineError } = useGetTimelineQuery({
        skip: !open || !siblingUrn,
        variables: {
            input: {
                urn: siblingUrn || '',
                changeCategories: allCategoryValues as ChangeCategoryType[],
            },
        },
    });

    const hasError = !!entityTimelineError || !!siblingTimelineError;

    const { entityPlatform, siblingPlatform } = useGetSiblingPlatforms();

    const allTransactions = useMemo(
        () => [
            ...(entityTimelineData?.getTimeline?.changeTransactions ?? []),
            ...(siblingTimelineData?.getTimeline?.changeTransactions ?? []),
        ],
        [entityTimelineData, siblingTimelineData],
    );
    const nameMap = useResolveEntityNames(allTransactions);

    const allEntries: ChangeTransactionEntry[] = useMemo(
        () =>
            [
                ...(entityTimelineData?.getTimeline?.changeTransactions?.map((transaction) =>
                    makeTransactionEntry(
                        transaction,
                        hideSemanticVersions ? [] : versionList,
                        entityPlatform ?? undefined,
                        nameMap,
                    ),
                ) || []),
                ...(siblingTimelineData?.getTimeline?.changeTransactions?.map((transaction) =>
                    makeTransactionEntry(transaction, [], siblingPlatform ?? undefined, nameMap),
                ) || []),
            ].sort((a, b) => a.transaction.timestampMillis - b.transaction.timestampMillis),
        [
            entityTimelineData,
            siblingTimelineData,
            versionList,
            hideSemanticVersions,
            entityPlatform,
            siblingPlatform,
            nameMap,
        ],
    );

    const filteredEntries = useMemo(() => {
        if (selectedCategories.length === allCategoryValues.length) return allEntries;
        const selected = new Set(selectedCategories);
        return allEntries.filter((entry) =>
            entry.transaction.changes?.some((c) => c?.category && selected.has(c.category)),
        );
    }, [allEntries, selectedCategories, allCategoryValues.length]);

    const entityCount = entityTimelineData?.getTimeline?.changeTransactions?.length ?? 0;
    const siblingCount = siblingTimelineData?.getTimeline?.changeTransactions?.length ?? 0;
    const mayBeTruncated = entityCount >= MAX_CHANGE_TRANSACTIONS || siblingCount >= MAX_CHANGE_TRANSACTIONS;

    return (
        <StyledDrawer
            open={open}
            onClose={() => onClose()}
            getContainer={() => document.getElementById('entity-profile-sidebar') as HTMLElement}
            contentWrapperStyle={{ width: '33%' }}
            mask={false}
            maskClosable={false}
            placement="right"
            closable={false}
            autoFocus={false}
        >
            <DrawerContent>
                <FieldHeaderWrapper>
                    Change History
                    <SimpleSelect
                        placeholder="Filter"
                        selectLabelProps={{ variant: 'labeled', label: 'Types' }}
                        options={categoryOptions}
                        values={selectedCategories}
                        onUpdate={(values) => setSelectedCategories(values)}
                        width="fit-content"
                        showClear={false}
                        isMultiSelect
                        showSelectAll
                    />
                    <CloseIcon onClick={() => onClose()}>
                        <CloseOutlinedIcon />
                    </CloseIcon>
                </FieldHeaderWrapper>

                <ChangeTransactionList>
                    {filteredEntries
                        .map((entry) => (
                            <ChangeTransactionView
                                key={`${entry.transaction.timestampMillis}-${entry.transaction.actor}`}
                                {...entry}
                            />
                        ))
                        .reverse()}
                </ChangeTransactionList>

                <HistoryFooter>
                    {hasError && 'Unable to load change history.'}
                    {!hasError &&
                        mayBeTruncated &&
                        'Showing the most recent changes. Older history may not be displayed.'}
                    {!hasError && !mayBeTruncated && 'Complete change history'}
                </HistoryFooter>
            </DrawerContent>
        </StyledDrawer>
    );
};

function makeTransactionEntry(
    transaction: ChangeTransaction,
    versionList: SemanticVersionStruct[],
    platform?: DataPlatform,
    nameMap?: Map<string, string>,
): ChangeTransactionEntry {
    return {
        transaction,
        platform,
        nameMap,
        semanticVersion:
            versionList.find((v) => v.semanticVersionTimestamp === transaction.timestampMillis)?.semanticVersion ??
            undefined,
    };
}

export default HistorySidebar;
