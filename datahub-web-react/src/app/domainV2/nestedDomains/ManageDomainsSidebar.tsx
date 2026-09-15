import { Avatar, Tooltip } from '@components';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { matchPath, useLocation } from 'react-router-dom';
import styled from 'styled-components';

import { AvatarType } from '@components/components/AvatarStack/types';
import { SimpleSelect } from '@components/components/Select/SimpleSelect';

import CreateDomainModal from '@app/domainV2/CreateDomainModal';
import DomainSearch from '@app/domainV2/DomainSearch';
import DomainNavigator from '@app/domainV2/nestedDomains/domainNavigator/DomainNavigator';
import {
    DomainSidebarFiltersProvider,
    useDomainSidebarFilters,
} from '@app/domainV2/nestedDomains/domainSidebarFilters/DomainSidebarFiltersContext';
import {
    DOMAIN_SIDEBAR_SORT,
    DomainSidebarSortValue,
} from '@app/domainV2/nestedDomains/domainSidebarFilters/domainSidebarSort';
import HierarchicalBrowseSidebar from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseSidebar';
import { SidebarCreateButton } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseSidebar.components';
import SidebarHomeNavLink from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/SidebarHomeNavLink';
import SidebarSortSelect from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/SidebarSortSelect';
import { PageRoutes } from '@conf/Global';

import { EntityType } from '@types';

const OwnerOptionRow = styled.span`
    display: flex;
    align-items: center;
    gap: 8px;
`;

function ManageDomainsSidebarInner() {
    const { t } = useTranslation('governance.domain');
    const { t: tm } = useTranslation('misc');
    const location = useLocation();
    const [isClosed, setIsClosed] = useState(false);
    const [isCreatingDomain, setIsCreatingDomain] = useState(false);
    const { selectedOwnerUrns, setSelectedOwnerUrns, availableOwners, sortSelection, setSortSelection } =
        useDomainSidebarFilters();
    const isFirstSortEffectRef = useRef(true);

    const isHomeSelected = matchPath(location.pathname, { path: PageRoutes.DOMAINS, exact: true }) !== null;

    // Sort reloads the scroll stream from page 1 — pin the browse list at the top
    // so results don't land off-screen after the user had scrolled down (Documents).
    useEffect(() => {
        if (isFirstSortEffectRef.current) {
            isFirstSortEffectRef.current = false;
            return;
        }
        const treeScroll = document.querySelector('[data-testid="hierarchical-browse-tree-scroll"]');
        if (treeScroll instanceof HTMLElement) {
            treeScroll.scrollTop = 0;
        }
    }, [sortSelection]);

    const unhideSidebar = useCallback(() => {
        setIsClosed(false);
    }, []);

    const ownerOptions = useMemo(
        () =>
            availableOwners.map((owner) => ({
                value: owner.urn,
                label: owner.displayName,
                owner,
            })),
        [availableOwners],
    );

    const sortOptions = useMemo(
        () => [
            { value: DOMAIN_SIDEBAR_SORT.NAME_ASC, label: tm('sidebarSort.nameAtoZ') },
            { value: DOMAIN_SIDEBAR_SORT.NAME_DESC, label: tm('sidebarSort.nameZtoA') },
            { value: DOMAIN_SIDEBAR_SORT.CREATED_DESC, label: tm('sidebarSort.created') },
        ],
        [tm],
    );

    const headerActions = (
        <Tooltip showArrow={false} title={t('sidebar.createTooltip')} placement="right">
            <SidebarCreateButton
                variant="filled"
                color="primary"
                isCircle
                icon={{ icon: Plus }}
                onClick={() => setIsCreatingDomain(true)}
                data-testid="sidebar-create-domain-button"
            />
        </Tooltip>
    );

    // Domains only support Owners (no tags/terms on the Domain entity). One
    // filter → no "+ Filter" menu.
    const filters = (
        <SimpleSelect
            size="sm"
            width="fit-content"
            isMultiSelect
            showSearch
            filterResultsByQuery
            isDisabled={ownerOptions.length === 0}
            placeholder={t('navigator.ownerFilter.placeholder')}
            selectLabelProps={{
                variant: 'labeled',
                label: t('navigator.ownerFilter.label'),
            }}
            options={ownerOptions}
            values={selectedOwnerUrns}
            onUpdate={setSelectedOwnerUrns}
            renderCustomOptionText={(option) => {
                const { owner } = option as (typeof ownerOptions)[number];
                return (
                    <OwnerOptionRow>
                        <Avatar
                            name={owner.displayName}
                            imageUrl={owner.pictureLink ?? undefined}
                            type={owner.type === EntityType.CorpGroup ? AvatarType.group : AvatarType.user}
                            showInPill
                            size="sm"
                        />
                    </OwnerOptionRow>
                );
            }}
            dataTestId="domain-sidebar-owner-filter"
        />
    );

    return (
        <>
            <HierarchicalBrowseSidebar
                title={t('page.title')}
                isCollapsed={isClosed}
                onToggleCollapsed={() => setIsClosed((prev) => !prev)}
                onExpandSidebar={unhideSidebar}
                headerActions={headerActions}
                id="browse-v2"
                search={<DomainSearch />}
                sort={
                    <SidebarSortSelect
                        options={sortOptions}
                        value={sortSelection}
                        onChange={(next) => setSortSelection(next as DomainSidebarSortValue)}
                        dataTestId="domain-sidebar-sort"
                    />
                }
                filters={filters}
                homeNav={
                    <SidebarHomeNavLink
                        to={PageRoutes.DOMAINS}
                        isSelected={isHomeSelected}
                        label={t('navigator.overview')}
                        data-testid="domain-sidebar-overview"
                    />
                }
            >
                {({ isCollapsed }) =>
                    isCollapsed ? (
                        <>
                            <DomainSearch isCollapsed unhideSidebar={unhideSidebar} />
                            <DomainNavigator key={sortSelection} isCollapsed variant="sidebar" includeHome />
                        </>
                    ) : (
                        <DomainNavigator key={sortSelection} variant="sidebar" includeHome={false} />
                    )
                }
            </HierarchicalBrowseSidebar>
            {isCreatingDomain && (
                <CreateDomainModal
                    onClose={() => setIsCreatingDomain(false)}
                    onCreate={() => setIsCreatingDomain(false)}
                />
            )}
        </>
    );
}

export default function ManageDomainsSidebarV2() {
    return (
        <DomainSidebarFiltersProvider>
            <ManageDomainsSidebarInner />
        </DomainSidebarFiltersProvider>
    );
}
