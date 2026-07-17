import { Alert, EmptyState } from '@components';
import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import { Folder } from '@phosphor-icons/react/dist/csr/Folder';
import { House } from '@phosphor-icons/react/dist/csr/House';
import React, { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { matchPath, useHistory, useLocation } from 'react-router-dom';
import styled, { useTheme } from 'styled-components';

import DomainFlatItem from '@app/domainV2/nestedDomains/domainNavigator/DomainFlatItem';
import DomainNode from '@app/domainV2/nestedDomains/domainNavigator/DomainNode';
import { useDomainSidebarFilters } from '@app/domainV2/nestedDomains/domainSidebarFilters/DomainSidebarFiltersContext';
import useDomainOwnerAggregations from '@app/domainV2/nestedDomains/domainSidebarFilters/useDomainOwnerAggregations';
import { DomainNavigatorVariant } from '@app/domainV2/nestedDomains/types';
import useScrollDomains from '@app/domainV2/useScrollDomains';
import Loading from '@app/shared/Loading';
import { PageRoutes } from '@conf/Global';

import { Domain } from '@types';

// Sidebar variant gets an 8px gutter on all sides — matches the documents
// sidebar's TreeContainer exactly so the row chrome (selected highlight,
// hover shadow) sits inset from the sidebar edge AND from the divider above.
// The divider that separates this region from the filters lives in the
// parent (ManageDomainsSidebar) so it spans the full sidebar width, mirroring
// the documents sidebar layout. Select variant stays flush since its rows
// have full-width bottom borders.
const NavigatorWrapper = styled.div<{ $isSidebar: boolean }>`
    font-size: 14px;
    max-height: calc(100% - 65px);
    overflow: auto;
    ${(props) => props.$isSidebar && 'padding: 8px;'}
`;

const LoadingWrapper = styled.div`
    padding: 16px;
`;

// --- Home / overview row ----------------------------------------------------
// Top-of-tree "All Domains" navigation entry. Styled to mirror the sibling
// rows from DocumentTreeItem (38px row, 6px radius, 8px left padding, brand-
// gradient selected text) so it reads as a peer of the domain tree rows. Acts
// as the sidebar's home link to the /domains index page.

// `$isCollapsed` swaps the row from the expanded "icon + label" layout to a
// single centered icon — same treatment as `SidebarRowContainer` in
// `DomainNode`, so the home row aligns vertically with every domain row in
// the collapsed sidebar's 63px column.
const OverviewRow = styled.div<{ $isSelected: boolean; $isCollapsed: boolean }>`
    position: relative;
    display: flex;
    align-items: center;
    justify-content: ${(props) => (props.$isCollapsed ? 'center' : 'flex-start')};
    padding: ${(props) => (props.$isCollapsed ? '4px 0' : '4px 8px')};
    min-height: 38px;
    height: 38px;
    cursor: pointer;
    border-radius: 6px;
    transition: background-color 0.15s ease;
    /* Row sits flush against the NavigatorWrapper's 8px top padding, exactly
       like DocumentTreeItem sits against TreeContainer. Small horizontal
       margin matches sibling DomainNode rows so the selected/hover shadow
       isn't clipped against the wrapper's padding edge. */
    margin: 0 2px 2px 2px;

    ${(props) =>
        props.$isSelected &&
        `
        background: ${props.theme.colors.bgSelectedSubtle};
        box-shadow: ${props.theme.colors.shadowFocusBrand};
    `}

    ${(props) =>
        !props.$isSelected &&
        `
        &:hover {
            background: ${props.theme.colors.bgHover};
            box-shadow: ${props.theme.colors.shadowFocus};
        }
    `}
`;

// Drop the trailing 8px gutter in collapsed mode — with no label next to it,
// the gutter would push the icon visibly right of center.
const OverviewIconSlot = styled.div<{ $isCollapsed: boolean }>`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 24px;
    height: 20px;
    margin-right: ${(props) => (props.$isCollapsed ? '0' : '8px')};
    flex-shrink: 0;
`;

const OverviewLabel = styled.span<{ $isSelected: boolean }>`
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    font-size: 14px;
    line-height: 20px;
    color: ${(props) => props.theme.colors.textSecondary};

    ${(props) =>
        props.$isSelected &&
        `
        background: ${props.theme.colors.brandGradientSelected};
        background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: 600;
    `}
`;

// --- Section header ---------------------------------------------------------
// "All Domains" group label at the top of the tree. Styling intentionally
// matches the documents sidebar's per-platform `SectionHeader` (DataHub /
// GitHub / Google Docs / …) so the two trees read as siblings — same
// Mulish-700 / textTertiary treatment, same right-side caret, same 32px
// minimum row height, same indent formula (8 + level*16). Acts as a pure
// collapsible group header (no navigation), again matching documents.
const SectionHeader = styled.button<{ $level: number }>`
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
    width: 100%;
    padding: 6px 8px 6px ${(props) => 8 + props.$level * 16}px;
    min-height: 32px;
    border: none;
    background: transparent;
    cursor: pointer;
    text-align: left;
    color: ${(props) => props.theme.colors.textTertiary};
    font-family: Mulish;
    font-size: 14px;
    font-weight: 700;
`;

const SectionHeaderLabel = styled.span`
    display: flex;
    align-items: center;
    gap: 8px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

/**
 * Collapsible section header — pure presentation, no expansion state of its
 * own. Mirrors `TreeSectionHeader` from the documents sidebar's `DocumentTree`
 * so a level-0 "All Domains" header lines up with a level-0 "DataHub" /
 * "GitHub" header pixel-for-pixel.
 */
function DomainSectionHeader({
    level,
    label,
    isExpanded,
    onToggle,
    testId,
}: {
    level: number;
    label: string;
    isExpanded: boolean;
    onToggle: () => void;
    testId?: string;
}) {
    const Chevron = isExpanded ? CaretDown : CaretRight;
    return (
        <SectionHeader type="button" $level={level} onClick={onToggle} aria-expanded={isExpanded} data-testid={testId}>
            <SectionHeaderLabel>{label}</SectionHeaderLabel>
            <Chevron size={14} weight="regular" />
        </SectionHeader>
    );
}

interface Props {
    domainUrnToHide?: string;
    selectDomainOverride?: (domain: Domain) => void;
    isCollapsed?: boolean;
    variant?: DomainNavigatorVariant;
}

export default function DomainNavigator({
    domainUrnToHide,
    isCollapsed,
    selectDomainOverride,
    variant = 'select',
}: Props) {
    const { t } = useTranslation('governance.domain');
    const { selectedOwnerUrns, setAvailableOwners } = useDomainSidebarFilters();
    const isSidebar = variant === 'sidebar';

    // When the sidebar's owner filter is active, we swap the recursive tree
    // for a flat search-style result list. The tree mode would otherwise
    // silently hide matching subdomains whose ancestors don't match the
    // filter (e.g. selecting an owner who only owns one deeply-nested
    // domain returned "No Domains Found" because the root-level query came
    // back empty — bug surfaced in PR #18088 review).
    const isFiltering = isSidebar && selectedOwnerUrns.length > 0;

    // `ignoreParentScope: true` drops the `parentDomain NOT EXISTS` clause
    // so the scroll spans every domain at every depth. Picker variants
    // never get the owner selection — they own their own scope.
    const { domains, hasInitialized, loading, error, scrollRef } = useScrollDomains({
        selectedOwnerUrns: isSidebar ? selectedOwnerUrns : undefined,
        ignoreParentScope: isFiltering,
    });

    // Dropdown options come from a dedicated aggregation query that covers
    // every domain in the index (no parentDomain filter, no owners filter).
    // Sourcing them from the scroll query's facets would only ever see
    // owners of root domains — owners attached only to subdomains, or only
    // to domains that haven't paged into view yet, would silently disappear
    // from the filter.
    const { owners: aggregatedOwners } = useDomainOwnerAggregations({ skip: !isSidebar });

    const theme = useTheme();
    const history = useHistory();
    const location = useLocation();

    // Home / overview row navigation. Selected when the user is on /domains.
    // Navigates in both collapsed and expanded modes — collapsed-mode clicks
    // on a recognizable icon should follow through to the target page, not
    // get redirected into a sidebar-expand action.
    const isOnOverview = isSidebar && matchPath(location.pathname, { path: PageRoutes.DOMAINS, exact: true }) !== null;
    const handleOverviewClick = () => {
        history.push(PageRoutes.DOMAINS);
    };

    // Section expansion state — local to the component. Defaults open; toggling
    // the "All Domains" header hides the tree (matches the docs sidebar's
    // "DataHub" / "GitHub" headers, which collapse their groups in place).
    const [isAllDomainsExpanded, setIsAllDomainsExpanded] = useState(true);

    // Mirror the aggregation result into the shared context so the
    // SimpleSelect in `ManageDomainsSidebar` picks it up. The aggregation
    // query is cache-first with `previousData` fallback, so this stays
    // stable across the scroll-query refetches that the owner filter
    // triggers.
    useEffect(() => {
        if (!isSidebar) return;
        setAvailableOwners(aggregatedOwners);
    }, [isSidebar, aggregatedOwners, setAvailableOwners]);

    const showTreeContents = !isSidebar || isCollapsed || isAllDomainsExpanded;
    const showEmptyState = hasInitialized && (domains?.length ?? 0) === 0 && showTreeContents;

    // Section header is hidden in collapsed mode (no labels at all in the
    // 63px column) and when filtering (the flat list IS the filter result
    // — a header above it would be redundant and slightly misleading).
    const showSectionHeader = isSidebar && !isCollapsed && !isFiltering;

    return (
        <NavigatorWrapper $isSidebar={isSidebar}>
            {isSidebar && (
                <OverviewRow
                    $isSelected={isOnOverview}
                    $isCollapsed={!!isCollapsed}
                    onClick={handleOverviewClick}
                    data-testid="domain-sidebar-overview"
                >
                    <OverviewIconSlot $isCollapsed={!!isCollapsed}>
                        <House
                            size={18}
                            weight={isOnOverview ? 'fill' : 'regular'}
                            color={isOnOverview ? theme.colors.iconBrand : theme.colors.icon}
                        />
                    </OverviewIconSlot>
                    {!isCollapsed && (
                        <OverviewLabel $isSelected={isOnOverview}>{t('navigator.overview')}</OverviewLabel>
                    )}
                </OverviewRow>
            )}
            {showSectionHeader && (
                <DomainSectionHeader
                    level={0}
                    label={t('navigator.section.allDomains')}
                    isExpanded={isAllDomainsExpanded}
                    onToggle={() => setIsAllDomainsExpanded((v) => !v)}
                    testId="domain-sidebar-section-all-domains"
                />
            )}
            {error && <Alert variant="error" title={t('navigator.loadError')} />}
            {showEmptyState && (
                <EmptyState
                    title={isFiltering ? t('navigator.emptyFiltered') : t('navigator.empty')}
                    icon={Folder}
                    size="sm"
                />
            )}
            {showTreeContents &&
                domains?.map((domain) =>
                    isFiltering ? (
                        <DomainFlatItem key={domain.urn} domain={domain} />
                    ) : (
                        <DomainNode
                            key={domain.urn}
                            domain={domain as Domain}
                            numDomainChildren={domain.children?.total || 0}
                            domainUrnToHide={domainUrnToHide}
                            selectDomainOverride={selectDomainOverride}
                            isCollapsed={isCollapsed}
                            level={0}
                            variant={variant}
                        />
                    ),
                )}
            {loading && showTreeContents && (
                <LoadingWrapper>
                    <Loading height={24} marginTop={0} />
                </LoadingWrapper>
            )}
            {showTreeContents && (domains?.length ?? 0) > 0 && <div ref={scrollRef} />}
        </NavigatorWrapper>
    );
}
