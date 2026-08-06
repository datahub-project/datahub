import { Avatar, Tooltip } from '@components';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import React, { useCallback, useMemo, useState } from 'react';
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
import HierarchicalBrowseSidebar from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseSidebar';
import { SidebarCreateButton } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseSidebar.components';
import SidebarHomeNavLink from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/SidebarHomeNavLink';
import { PageRoutes } from '@conf/Global';

import { EntityType } from '@types';

const OwnerOptionRow = styled.span`
    display: flex;
    align-items: center;
    gap: 8px;
`;

function ManageDomainsSidebarInner() {
    const { t } = useTranslation('governance.domain');
    const location = useLocation();
    const [isClosed, setIsClosed] = useState(false);
    const [isCreatingDomain, setIsCreatingDomain] = useState(false);
    const { selectedOwnerUrns, setSelectedOwnerUrns, availableOwners } = useDomainSidebarFilters();

    const isHomeSelected = matchPath(location.pathname, { path: PageRoutes.DOMAINS, exact: true }) !== null;

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

    const ownerFilter = (
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
                filters={ownerFilter}
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
                            <DomainNavigator isCollapsed variant="sidebar" includeHome />
                        </>
                    ) : (
                        <DomainNavigator variant="sidebar" includeHome={false} />
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
