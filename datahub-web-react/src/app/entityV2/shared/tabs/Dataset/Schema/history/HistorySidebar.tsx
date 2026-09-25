import CloseOutlinedIcon from '@mui/icons-material/CloseOutlined';
import { Drawer } from 'antd';
import React, { useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { SearchBar } from '@components/components/SearchBar/SearchBar';
import { SimpleSelect } from '@components/components/Select/SimpleSelect';

import { useGetSiblingPlatforms } from '@app/entity/shared/siblingUtils';
import ChangeTransactionView, {
    ChangeTransactionEntry,
    VersionEntry,
} from '@app/entityV2/shared/tabs/Dataset/Schema/history/ChangeTransactionView';
import {
    PARAM_DESCRIPTION,
    getCategoryOptions,
} from '@app/entityV2/shared/tabs/Dataset/Schema/history/HistorySidebar.utils';
import { getChangeEventString } from '@app/entityV2/shared/tabs/Dataset/Schema/history/changeEventToString';
import { useResolveEntityNames } from '@app/entityV2/shared/tabs/Dataset/Schema/history/useResolveEntityNames';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetTimelineQuery } from '@graphql/timeline.generated';
import { useGetVersionSetLatestQuery } from '@graphql/versioning.generated';
import {
    ChangeCategoryType,
    ChangeOperationType,
    ChangeTransaction,
    DataPlatform,
    EntityType,
    SemanticVersionStruct,
} from '@types';

/** Pulls structured version-milestone data out of a transaction's VERSIONING change event.
 *  Returns null when the transaction isn't a version-creation milestone — i.e. either no
 *  VERSIONING-category change is present, or one is present but lacks the structured parameters
 *  (which can happen for events emitted by older servers). In that fallback case the transaction
 *  still renders as a plain change row using its description text.
 */
function extractVersionEntry(
    transaction: ChangeTransaction,
    currentEntityUrn: string,
    buildEntityPath?: (urn: string) => string | undefined,
): VersionEntry | undefined {
    const versioningChange = (transaction.changes ?? []).find((c) => c?.category === ChangeCategoryType.Versioning);
    if (!versioningChange) return undefined;

    const params = new Map<string, string>(
        (versioningChange.parameters ?? []).map((p) => [p.key ?? '', p.value ?? '']),
    );
    const tag = params.get('versionTag');
    if (!tag) return undefined; // Unstructured event — let the default row render.

    const versionUrn = versioningChange.urn ?? currentEntityUrn;
    const isCurrent = versionUrn === currentEntityUrn;

    return {
        urn: versionUrn,
        tag,
        comment: params.get('comment') ?? null,
        isCurrent,
        isLatest: params.get('isLatest') === 'true',
        lifecycleStage: null,
        entityPath: isCurrent ? undefined : buildEntityPath?.(versionUrn),
    };
}

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

/** Scope indicator bar — sits between the drawer header and the filter row.
 *  Only rendered for versioned entities (versionSetUrn truthy).
 *  Inactive: white background, quiet invitation to expand scope.
 *  Active:   violet left-accent + tinted background — unmistakably in "all-versions" mode.
 */
const VersionScopeBar = styled.div<{ $active?: boolean }>`
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 0 16px;
    height: 36px;
    flex-shrink: 0;
    cursor: pointer;
    user-select: none;
    border-left: 3px solid ${({ $active, theme }) => ($active ? theme.colors.borderBrand : 'transparent')};
    background: ${({ $active, theme }) => ($active ? theme.colors.bgSurfaceBrand : theme.colors.bgSurface)};
    border-bottom: 1px solid ${({ theme }) => theme.colors.border};
    transition:
        background 0.18s ease,
        border-left-color 0.18s ease,
        border-bottom-color 0.18s ease;

    &:hover {
        background: ${({ theme }) => theme.colors.bgSurfaceBrandHover};
    }
`;

const ScopeLeft = styled.div`
    display: flex;
    align-items: center;
    gap: 7px;
`;

const ScopeIcon = styled.span<{ $active?: boolean }>`
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 16px;
    height: 16px;
    flex-shrink: 0;
    color: ${({ $active, theme }) => ($active ? theme.colors.textBrand : theme.colors.textTertiary)};
    transition: color 0.18s ease;
`;

const ScopeLabel = styled.span<{ $active?: boolean }>`
    font-family:
        'Mulish',
        -apple-system,
        sans-serif;
    font-size: 11.5px;
    font-weight: ${({ $active }) => ($active ? 700 : 500)};
    color: ${({ $active, theme }) => ($active ? theme.colors.text : theme.colors.textTertiary)};
    letter-spacing: 0.01em;
    transition:
        color 0.18s ease,
        font-weight 0.18s ease;
`;

const ScopeAction = styled.span<{ $active?: boolean }>`
    font-family:
        'Mulish',
        -apple-system,
        sans-serif;
    font-size: 11px;
    font-weight: 600;
    color: ${({ theme }) => theme.colors.textBrand};
    letter-spacing: 0.01em;
    display: flex;
    align-items: center;
    gap: 3px;
    transition: color 0.18s ease;

    &:hover {
        color: ${({ theme }) => theme.colors.textHover};
        text-decoration: underline;
        text-underline-offset: 2px;
    }
`;

const VersionCountBadge = styled.span`
    font-size: 10.5px;
    font-weight: 500;
    color: ${({ theme }) => theme.colors.textTertiary};
    margin-left: 2px;
`;

const EmptyVersionHistory = styled.div`
    padding: 24px 0;
    text-align: center;
    font-size: 13px;
    color: ${({ theme }) => theme.colors.textTertiary};
`;

interface Props {
    open: boolean;
    onClose: () => void;
    urn: string;
    siblingUrn?: string;
    versionList: SemanticVersionStruct[];
    hideSemanticVersions: boolean;
    entityType?: EntityType;
    defaultCategories?: string[];
    /** When provided the sidebar shows a "Show all versions" toggle. */
    versionSetUrn?: string;
    currentVersionUrn?: string;
    /** When true, the sidebar opens pre-set to the "All versions" concept history view. */
    defaultShowAllVersions?: boolean;
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
    versionSetUrn,
    defaultShowAllVersions,
}: Props) => {
    const { t } = useTranslation('entity.profile.schema');
    const entityRegistry = useEntityRegistry();
    const buildEntityPath = useMemo(
        () => (entityUrn: string) => {
            if (!entityType) return undefined;
            try {
                return entityRegistry.getEntityUrl(entityType, entityUrn);
            } catch (e) {
                return undefined;
            }
        },
        [entityRegistry, entityType],
    );
    const categoryOptions = useMemo(() => getCategoryOptions(entityType), [entityType]);
    const allCategoryValues = useMemo(() => categoryOptions.map((o) => o.value), [categoryOptions]);
    const initialCategories = defaultCategories ?? allCategoryValues;
    const [selectedCategories, setSelectedCategories] = useState<string[]>(initialCategories);
    const [searchText, setSearchText] = useState('');
    const [showAllVersions, setShowAllVersions] = useState(defaultShowAllVersions ?? false);

    useEffect(() => setShowAllVersions(defaultShowAllVersions ?? false), [defaultShowAllVersions]);

    useEffect(
        () => setSelectedCategories(defaultCategories ?? allCategoryValues),
        [defaultCategories, allCategoryValues],
    );

    // ── Main timeline query — optionally merges all sibling versions ──────────

    const { data: entityTimelineData, error: entityTimelineError } = useGetTimelineQuery({
        skip: !open,
        variables: {
            input: {
                urn,
                changeCategories: allCategoryValues as ChangeCategoryType[],
                includeVersionSet: showAllVersions && !!versionSetUrn,
            },
        },
    });

    // ── Optional sibling (for dataset twin / schema sibling, not versioning) ──

    const { data: siblingTimelineData, error: siblingTimelineError } = useGetTimelineQuery({
        skip: !open || !siblingUrn,
        variables: {
            input: {
                urn: siblingUrn || '',
                changeCategories: allCategoryValues as ChangeCategoryType[],
            },
        },
    });

    // Fetch the version count (always, when sidebar is open) and the authoritative latest-version
    // URN (used to fix stale isLatest badges when the all-versions view is active).
    const { data: versionSetLatestData } = useGetVersionSetLatestQuery({
        skip: !open || !versionSetUrn,
        variables: { versionSetUrn: versionSetUrn ?? '' },
    });
    const latestVersionUrn = showAllVersions ? (versionSetLatestData?.versionSet?.latestVersion?.urn ?? null) : null;
    const versionCount = versionSetLatestData?.versionSet?.versionsSearch?.total ?? null;

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

    const allEntries: ChangeTransactionEntry[] = useMemo(() => {
        const sorted = [
            ...(entityTimelineData?.getTimeline?.changeTransactions?.map((transaction) => ({
                ...makeTransactionEntry(
                    transaction,
                    hideSemanticVersions ? [] : versionList,
                    entityPlatform ?? undefined,
                    nameMap,
                ),
                versionEntry: extractVersionEntry(transaction, urn, buildEntityPath),
            })) || []),
            ...(siblingTimelineData?.getTimeline?.changeTransactions?.map((transaction) =>
                makeTransactionEntry(transaction, [], siblingPlatform ?? undefined, nameMap),
            ) || []),
        ].sort((a, b) => {
            const timeDiff = a.transaction.timestampMillis - b.transaction.timestampMillis;
            if (timeDiff !== 0) return timeDiff;
            // Tie-break: VERSIONING stamps come last within the same ms (metadata precedes version creation).
            const aIsVersioning = a.versionEntry != null;
            const bIsVersioning = b.versionEntry != null;
            if (aIsVersioning && !bIsVersioning) return 1;
            if (!aIsVersioning && bIsVersioning) return -1;
            return 0;
        });

        // Compute base entries: single-version mode uses `sorted` as-is; all-versions mode
        // corrects isLatest and attaches ownerVersion pills to non-milestone entries.
        const baseEntries: ChangeTransactionEntry[] = (() => {
            if (!showAllVersions) return sorted;

            // In all-versions mode:
            //  1. Override the historical isLatest from MCL events with the authoritative VersionSet.latest.
            //     Historical MCL events can have stale isLatest=true values (e.g. if entities were created
            //     before the VersionSet was anchored, so the server had no reference to compute correctly).
            //  2. Tag each non-milestone entry with its owning version for the inline version pill.
            const correctedSorted = latestVersionUrn
                ? sorted.map((e) => {
                      if (!e.versionEntry) return e;
                      return {
                          ...e,
                          versionEntry: { ...e.versionEntry, isLatest: e.versionEntry.urn === latestVersionUrn },
                      };
                  })
                : sorted;

            const urnToVersion = new Map<string, VersionEntry>();
            correctedSorted.forEach((e) => {
                if (e.versionEntry) urnToVersion.set(e.versionEntry.urn, e.versionEntry);
            });
            return correctedSorted.map((e) => {
                if (e.versionEntry) return e;
                const entityUrn = e.transaction.changes?.[0]?.urn ?? '';
                const ownerVersion = urnToVersion.get(entityUrn);
                return ownerVersion ? { ...e, ownerVersion } : e;
            });
        })();

        // Walk chronologically (baseEntries is oldest-first) to attach inheritedPreviousDescription
        // to Documentation ADD events. In all-versions mode this lets ADD events show a diff against
        // the prior version's last known description instead of an all-green empty→new view.
        let lastKnownDescription = '';
        return baseEntries.map((entry) => {
            const docChange = (entry.transaction.changes ?? []).find(
                (c) => c?.category === ChangeCategoryType.Documentation,
            );
            if (!docChange) return entry;

            const descParam = (docChange.parameters ?? []).find((p) => p.key === PARAM_DESCRIPTION);
            const newDesc = descParam?.value ?? '';
            const isAdd = (docChange.operation as string) === ChangeOperationType.Add;

            let result = entry;
            if (isAdd && lastKnownDescription) {
                result = { ...entry, inheritedPreviousDescription: lastKnownDescription };
            }
            if (newDesc) lastKnownDescription = newDesc;
            return result;
        });
    }, [
        entityTimelineData,
        siblingTimelineData,
        versionList,
        hideSemanticVersions,
        entityPlatform,
        siblingPlatform,
        nameMap,
        urn,
        buildEntityPath,
        showAllVersions,
        latestVersionUrn,
    ]);

    const displayTexts = useMemo(
        () =>
            allEntries.map((entry) =>
                (entry.transaction.changes ?? [])
                    .map((change) => getChangeEventString(change, nameMap) ?? '')
                    .join(' '),
            ),
        [allEntries, nameMap],
    );

    const filteredEntries = useMemo(() => {
        const lowerSearch = searchText.toLowerCase().trim();
        const filterByCategory = selectedCategories.length !== allCategoryValues.length;
        const filterBySearch = lowerSearch.length > 0;

        if (!filterByCategory && !filterBySearch) return allEntries;

        const selected = new Set(selectedCategories);
        return allEntries.filter((entry, index) => {
            const changes = entry.transaction.changes ?? [];
            const matchesCategory = !filterByCategory || changes.some((c) => c?.category && selected.has(c.category));
            const matchesSearch = !filterBySearch || displayTexts[index]?.toLowerCase().includes(lowerSearch);
            return matchesCategory && matchesSearch;
        });
    }, [allEntries, selectedCategories, allCategoryValues, searchText, displayTexts]);

    const entityCount = entityTimelineData?.getTimeline?.changeTransactions?.length ?? 0;
    const siblingCount = siblingTimelineData?.getTimeline?.changeTransactions?.length ?? 0;
    const mayBeTruncated = entityCount >= MAX_CHANGE_TRANSACTIONS || siblingCount >= MAX_CHANGE_TRANSACTIONS;
    const skippedVersionCount = entityTimelineData?.getTimeline?.skippedVersionCount ?? 0;

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
            <DrawerContent data-testid="schema-blame-history-panel">
                <FieldHeaderWrapper>
                    {t('historySidebar.changeHistory')}
                    <CloseIcon data-testid="history-close-btn" onClick={() => onClose()}>
                        <CloseOutlinedIcon />
                    </CloseIcon>
                </FieldHeaderWrapper>

                {versionSetUrn && (
                    <VersionScopeBar
                        data-testid="version-scope-bar"
                        $active={showAllVersions}
                        onClick={() => setShowAllVersions((v) => !v)}
                    >
                        <ScopeLeft>
                            {/* Stacked-layers icon: single layer when scoped, triple when expanded */}
                            <ScopeIcon $active={showAllVersions}>
                                {showAllVersions ? (
                                    <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
                                        <rect
                                            x="2"
                                            y="9"
                                            width="10"
                                            height="2"
                                            rx="1"
                                            fill="currentColor"
                                            opacity="0.35"
                                        />
                                        <rect
                                            x="2"
                                            y="6"
                                            width="10"
                                            height="2"
                                            rx="1"
                                            fill="currentColor"
                                            opacity="0.65"
                                        />
                                        <rect x="2" y="3" width="10" height="2" rx="1" fill="currentColor" />
                                    </svg>
                                ) : (
                                    <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
                                        <rect
                                            x="2"
                                            y="6"
                                            width="10"
                                            height="2"
                                            rx="1"
                                            fill="currentColor"
                                            opacity="0.5"
                                        />
                                        <rect x="2" y="3" width="10" height="2" rx="1" fill="currentColor" />
                                    </svg>
                                )}
                            </ScopeIcon>
                            <ScopeLabel $active={showAllVersions}>
                                {showAllVersions
                                    ? t('historySidebar.scopeAllVersions')
                                    : t('historySidebar.scopeThisVersion')}
                            </ScopeLabel>
                            {!showAllVersions && versionCount !== null && versionCount > 1 && (
                                <VersionCountBadge>
                                    {t('historySidebar.versionCountHint', { count: versionCount })}
                                </VersionCountBadge>
                            )}
                        </ScopeLeft>
                        <ScopeAction $active={showAllVersions}>
                            {showAllVersions ? (
                                <>
                                    <svg width="10" height="10" viewBox="0 0 10 10" fill="none">
                                        <path
                                            d="M6 2L3 5L6 8"
                                            stroke="currentColor"
                                            strokeWidth="1.5"
                                            strokeLinecap="round"
                                            strokeLinejoin="round"
                                        />
                                    </svg>
                                    {t('historySidebar.backToThisVersion')}
                                </>
                            ) : (
                                <>
                                    {t('historySidebar.viewAllVersions')}
                                    <svg width="10" height="10" viewBox="0 0 10 10" fill="none">
                                        <path
                                            d="M4 2L7 5L4 8"
                                            stroke="currentColor"
                                            strokeWidth="1.5"
                                            strokeLinecap="round"
                                            strokeLinejoin="round"
                                        />
                                    </svg>
                                </>
                            )}
                        </ScopeAction>
                    </VersionScopeBar>
                )}

                <FilterBar>
                    <SearchBar
                        placeholder={t('historySidebar.searchChangesPlaceholder')}
                        value={searchText}
                        onChange={(val) => setSearchText(val)}
                        width="100%"
                        height="36px"
                    />
                    <SimpleSelect
                        placeholder={t('historySidebar.filterPlaceholder')}
                        selectLabelProps={{ variant: 'labeled', label: t('historySidebar.typesLabel') }}
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
                    {filteredEntries.length === 0 && showAllVersions && !hasError && (
                        <EmptyVersionHistory>{t('historySidebar.noVersionHistory')}</EmptyVersionHistory>
                    )}
                </ChangeTransactionList>

                <HistoryFooter>
                    {hasError && t('historySidebar.unableToLoad')}
                    {!hasError &&
                        showAllVersions &&
                        skippedVersionCount > 0 &&
                        t('historySidebar.skippedVersionsNotShown', { count: skippedVersionCount })}
                    {!hasError && mayBeTruncated && t('historySidebar.truncationNotice')}
                    {!hasError && !mayBeTruncated && skippedVersionCount === 0 && t('historySidebar.completeHistory')}
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
