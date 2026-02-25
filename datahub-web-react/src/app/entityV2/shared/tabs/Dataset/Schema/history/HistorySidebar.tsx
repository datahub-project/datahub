import CloseOutlinedIcon from '@mui/icons-material/CloseOutlined';
import { Drawer } from 'antd';
import React, { useMemo, useState } from 'react';
import styled from 'styled-components';

import { SimpleSelect } from '@components/components/Select/SimpleSelect';

import { useGetSiblingPlatforms } from '@app/entity/shared/siblingUtils';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import ChangeTransactionView, {
    ChangeTransactionEntry,
} from '@app/entityV2/shared/tabs/Dataset/Schema/history/ChangeTransactionView';

import { useGetTimelineQuery } from '@graphql/timeline.generated';
import { ChangeCategoryType, ChangeTransaction, DataPlatform, SemanticVersionStruct } from '@types';

const CATEGORY_OPTIONS = [
    { value: ChangeCategoryType.TechnicalSchema, label: 'Schema' },
    { value: ChangeCategoryType.Documentation, label: 'Documentation' },
    { value: ChangeCategoryType.Tag, label: 'Tags' },
    { value: ChangeCategoryType.GlossaryTerm, label: 'Terms' },
    { value: ChangeCategoryType.Ownership, label: 'Owners' },
    { value: ChangeCategoryType.Domain, label: 'Domains' },
    { value: ChangeCategoryType.StructuredProperty, label: 'Properties' },
];

const ALL_CATEGORY_VALUES = CATEGORY_OPTIONS.map((o) => o.value);

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
        box-shadow: -20px 0px 44px 0px rgba(0, 0, 0, 0.15);
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
    color: #fff;
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
        stroke: ${REDESIGN_COLORS.WHITE};
    }
`;

const HistoryFooter = styled.div`
    padding: 12px 26px;
    text-align: center;
    font-size: 12px;
    color: #8c8c8c;
    border-top: 1px solid #f0f0f0;
`;

interface Props {
    open: boolean;
    onClose: () => void;
    urn: string;
    siblingUrn?: string;
    versionList: SemanticVersionStruct[];
    hideSemanticVersions: boolean;
}

const HistorySidebar = ({ open, onClose, urn, siblingUrn, versionList, hideSemanticVersions }: Props) => {
    const [selectedCategories, setSelectedCategories] = useState<string[]>(ALL_CATEGORY_VALUES);

    const { data: entityTimelineData } = useGetTimelineQuery({
        skip: !open,
        variables: {
            input: {
                urn,
                changeCategories: ALL_CATEGORY_VALUES,
            },
        },
    });
    const { data: siblingTimelineData } = useGetTimelineQuery({
        skip: !open || !siblingUrn,
        variables: {
            input: {
                urn: siblingUrn || '',
                changeCategories: ALL_CATEGORY_VALUES,
            },
        },
    });

    const { entityPlatform, siblingPlatform } = useGetSiblingPlatforms();

    const allEntries: ChangeTransactionEntry[] = useMemo(
        () =>
            [
                ...(entityTimelineData?.getTimeline?.changeTransactions?.map((transaction) =>
                    makeTransactionEntry(
                        transaction,
                        hideSemanticVersions ? [] : versionList,
                        entityPlatform ?? undefined,
                    ),
                ) || []),
                ...(siblingTimelineData?.getTimeline?.changeTransactions?.map((transaction) =>
                    makeTransactionEntry(transaction, [], siblingPlatform ?? undefined),
                ) || []),
            ].sort((a, b) => a.transaction.timestampMillis - b.transaction.timestampMillis),
        [entityTimelineData, siblingTimelineData, versionList, hideSemanticVersions, entityPlatform, siblingPlatform],
    );

    const filteredEntries = useMemo(() => {
        if (selectedCategories.length === ALL_CATEGORY_VALUES.length) return allEntries;
        const selected = new Set(selectedCategories);
        return allEntries.filter((entry) =>
            entry.transaction.changes?.some((c) => c?.category && selected.has(c.category)),
        );
    }, [allEntries, selectedCategories]);

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
                        options={CATEGORY_OPTIONS}
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
                        .map((entry) => <ChangeTransactionView key={entry.transaction.versionStamp} {...entry} />)
                        .reverse()}
                </ChangeTransactionList>

                <HistoryFooter>
                    {mayBeTruncated
                        ? 'Showing the most recent changes. Older history may not be displayed.'
                        : 'Complete change history'}
                </HistoryFooter>
            </DrawerContent>
        </StyledDrawer>
    );
};

function makeTransactionEntry(
    transaction: ChangeTransaction,
    versionList: SemanticVersionStruct[],
    platform?: DataPlatform,
): ChangeTransactionEntry {
    return {
        transaction,
        platform,
        semanticVersion:
            versionList.find((v) => v.semanticVersionTimestamp === transaction.timestampMillis)?.semanticVersion ??
            undefined,
    };
}

export default HistorySidebar;
