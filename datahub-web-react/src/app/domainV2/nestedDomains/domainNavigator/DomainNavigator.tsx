import { Alert, EmptyState } from '@components';
import { Folder } from '@phosphor-icons/react/dist/csr/Folder';
import { House } from '@phosphor-icons/react/dist/csr/House';
import React, { useEffect, useRef, useState } from 'react';
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
import { TreeContainer } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseSidebar.components';
import HierarchicalBrowseTreeRow from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseTreeRow';
import {
    TreeExpansionRegistryProvider,
    useTreeExpansionRegistry,
} from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/TreeExpansionRegistry';
import { TreeSectionHeader } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/TreeSectionHeader';
import { TREE_ROW_ENTITY_ICON_SIZE } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/constants';
import { PageRoutes } from '@conf/Global';

import { Domain } from '@types';

// Select/picker variant stays flush (rows have full-width bottom borders).
const NavigatorWrapper = styled.div`
    font-size: 14px;
    max-height: calc(100% - 65px);
    overflow: auto;
    padding: 0;
`;

const LoadingWrapper = styled.div`
    padding: 16px;
`;

interface Props {
    domainUrnToHide?: string;
    selectDomainOverride?: (domain: Domain) => void;
    isCollapsed?: boolean;
    variant?: DomainNavigatorVariant;
    /**
     * When true (default for collapsed / picker), render the overview home row
     * inside the tree. Expanded sidebar sets this false — shell `homeNav` owns it.
     */
    includeHome?: boolean;
}

function DomainNavigatorInner({
    domainUrnToHide,
    isCollapsed,
    selectDomainOverride,
    variant = 'select',
    includeHome = true,
}: Props) {
    const { t } = useTranslation('governance.domain');
    const { t: tm } = useTranslation('misc');
    const { selectedOwnerUrns, setAvailableOwners, sortSelection } = useDomainSidebarFilters();
    const expansion = useTreeExpansionRegistry();
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
        sort: isSidebar ? sortSelection : undefined,
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
    // When expand-all runs while the section is collapsed, nodes aren't mounted
    // yet — defer expandAll until after they register (child effects run first).
    const pendingExpandAllRef = useRef(false);

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

    useEffect(() => {
        if (!pendingExpandAllRef.current || !isAllDomainsExpanded || !expansion) return;
        pendingExpandAllRef.current = false;
        expansion.expandAll();
    }, [isAllDomainsExpanded, expansion, domains]);

    const handleToggleExpandAll = () => {
        if (!expansion) return;
        if (expansion.hasAnyExpanded) {
            pendingExpandAllRef.current = false;
            expansion.collapseAll();
            return;
        }
        if (!isAllDomainsExpanded) {
            pendingExpandAllRef.current = true;
            setIsAllDomainsExpanded(true);
            return;
        }
        expansion.expandAll();
    };

    const showHomeRow = isSidebar && includeHome;

    const treeBody = (
        <>
            {showHomeRow && (
                <HierarchicalBrowseTreeRow
                    level={0}
                    isSelected={isOnOverview}
                    isCollapsed={!!isCollapsed}
                    icon={
                        <House
                            size={TREE_ROW_ENTITY_ICON_SIZE}
                            weight={isOnOverview ? 'fill' : 'regular'}
                            color={isOnOverview ? theme.colors.iconBrand : theme.colors.icon}
                        />
                    }
                    label={t('navigator.overview')}
                    onSelect={handleOverviewClick}
                    data-testid="domain-sidebar-overview"
                />
            )}
            {showSectionHeader && (
                <TreeSectionHeader
                    level={0}
                    label={t('navigator.section.allDomains')}
                    isExpanded={isAllDomainsExpanded}
                    onToggle={() => setIsAllDomainsExpanded((v) => !v)}
                    testId="domain-sidebar-section-all-domains"
                    onToggleExpandAll={handleToggleExpandAll}
                    isAllExpanded={expansion?.hasAnyExpanded}
                    expandAllLabel={tm('context.tree.expandAll')}
                    collapseAllLabel={tm('context.tree.collapseAll')}
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
        </>
    );

    // Expanded sidebar: shell HierarchicalBrowseSidebar owns scroll via TreeContainer.
    // Collapsed: local TreeContainer so the icon column can scroll.
    if (isSidebar) {
        if (isCollapsed) {
            return <TreeContainer>{treeBody}</TreeContainer>;
        }
        return <>{treeBody}</>;
    }

    return <NavigatorWrapper>{treeBody}</NavigatorWrapper>;
}

export default function DomainNavigator(props: Props) {
    if (props.variant === 'sidebar') {
        return (
            <TreeExpansionRegistryProvider>
                <DomainNavigatorInner {...props} />
            </TreeExpansionRegistryProvider>
        );
    }
    return <DomainNavigatorInner {...props} />;
}
