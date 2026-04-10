import CloseOutlinedIcon from '@mui/icons-material/CloseOutlined';
import { Drawer } from 'antd';
import React, { useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';

import { SearchBar } from '@components/components/SearchBar/SearchBar';
import { SimpleSelect } from '@components/components/Select/SimpleSelect';

import { useGetSiblingPlatforms } from '@app/entity/shared/siblingUtils';
import ChangeTransactionView, {
    ChangeTransactionEntry,
} from '@app/entityV2/shared/tabs/Dataset/Schema/history/ChangeTransactionView';
import {
    filterChangeEntries,
    getCategoryOptions,
} from '@app/entityV2/shared/tabs/Dataset/Schema/history/HistorySidebar.utils';
import { getChangeEventString } from '@app/entityV2/shared/tabs/Dataset/Schema/history/changeEventToString';
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
        box-shadow: ${(props) => props.theme.colors.shadowXl};
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
    background: ${(props) => props.theme.colors.buttonFillBrand};
    color: ${(props) => props.theme.colors.textOnFillBrand};
    font-size: 14px;
    font-weight: 700;
`;

const FilterBar = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 12px 16px;
    border-bottom: 1px solid ${(props) => props.theme.colors.border};
`;

const ChangeTransactionList = styled.div`
    display: flex;
    flex-direction: column;
    padding: 26px;
    overflow-y: auto;
    scrollbar-gutter: stable;
    flex: 1;
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
    /** When provided, only these categories are pre-selected on open (e.g., Schema tab passes [TECHNICAL_SCHEMA]). */
    defaultCategories?: string[];
}

const HistorySidebar = ({
    open,
    onClose,
    urn,
    siblingUrn,
    versionList,
    hideSemanticVersions,
    entityType,
    defaultCategories,
}: Props) => {
    const categoryOptions = useMemo(() => getCategoryOptions(entityType), [entityType]);
    const allCategoryValues = useMemo(() => categoryOptions.map((o) => o.value), [categoryOptions]);
    const initialCategories = defaultCategories ?? allCategoryValues;
    const [selectedCategories, setSelectedCategories] = useState<string[]>(initialCategories);
    const [searchText, setSearchText] = useState('');

    // Reset selection when the available categories change (e.g. navigating between entity types)
    useEffect(
        () => setSelectedCategories(defaultCategories ?? allCategoryValues),
        [defaultCategories, allCategoryValues],
    );

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

    // Pre-compute display strings (with resolved entity names) for search matching.
    // Each entry gets one concatenated string of all its change event display texts.
    const displayTexts = useMemo(
        () =>
            allEntries.map((entry) =>
                (entry.transaction.changes ?? [])
                    .map((change) => getChangeEventString(change, nameMap) ?? '')
                    .join(' '),
            ),
        [allEntries, nameMap],
    );

    const filteredEntries = useMemo(
        () => filterChangeEntries(allEntries, selectedCategories, allCategoryValues, searchText, displayTexts),
        [allEntries, selectedCategories, allCategoryValues, searchText, displayTexts],
    );

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
                    <CloseIcon onClick={() => onClose()}>
                        <CloseOutlinedIcon />
                    </CloseIcon>
                </FieldHeaderWrapper>
                <FilterBar>
                    <SearchBar
                        placeholder="Search changes..."
                        value={searchText}
                        onChange={(val) => setSearchText(val)}
                        width="100%"
                        height="36px"
                    />
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
                </FilterBar>

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
