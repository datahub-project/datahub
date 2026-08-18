import { Avatar, Menu } from '@components';
import { BookmarkSimple } from '@phosphor-icons/react/dist/csr/BookmarkSimple';
import { BookmarksSimple } from '@phosphor-icons/react/dist/csr/BookmarksSimple';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import React, { useCallback, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useLocation } from 'react-router-dom';
import styled from 'styled-components';

import { AvatarType } from '@components/components/AvatarStack/types';
import type { ItemType } from '@components/components/Menu/types';
import { SimpleSelect } from '@components/components/Select/SimpleSelect';

import { useUserContext } from '@app/context/useUserContext';
import { FacetSelectOption } from '@app/document/utils/documentSidebarFacets.utils';
import { isDomain } from '@app/entityV2/domain/utils';
import CreateGlossaryEntityModal from '@app/entityV2/shared/EntityDropdown/CreateGlossaryEntityModal';
import { useGlossaryEntityData } from '@app/entityV2/shared/GlossaryEntityContext';
import { isTag } from '@app/entityV2/tag/utils';
import GlossaryBrowser from '@app/glossaryV2/GlossaryBrowser/GlossaryBrowser';
import GlossaryColoredIcon from '@app/glossaryV2/GlossaryColoredIcon';
import GlossarySearch from '@app/glossaryV2/GlossarySearch';
import { useGenerateGlossaryColorFromPalette } from '@app/glossaryV2/colorUtils';
import {
    GlossarySidebarFiltersProvider,
    useGlossarySidebarFilters,
} from '@app/glossaryV2/glossarySidebarFilters/GlossarySidebarFiltersContext';
import {
    GLOSSARY_SIDEBAR_SORT,
    GlossarySidebarSortValue,
} from '@app/glossaryV2/glossarySidebarFilters/glossarySidebarSort';
import { getCollapsedGlossaryItems } from '@app/glossaryV2/utils';
import HierarchicalBrowseSidebar from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseSidebar';
import { SidebarCreateButton } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseSidebar.components';
import SidebarHomeNavLink from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/SidebarHomeNavLink';
import SidebarSortSelect from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/SidebarSortSelect';
import {
    TREE_ROW_ENTITY_ICON_GLYPH_SIZE,
    TREE_ROW_ENTITY_ICON_SIZE,
} from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/constants';
import { DomainLink } from '@app/sharedV2/tags/DomainLink';
import TagLink from '@app/sharedV2/tags/TagLink';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { PageRoutes } from '@conf/Global';

import { useGetRootGlossaryNodesQuery, useGetRootGlossaryTermsQuery } from '@graphql/glossary.generated';
import { EntityType } from '@types';

const OwnerOptionRow = styled.span`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const FiltersRow = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    align-items: center;
`;

function GlossarySidebarInner() {
    const { t } = useTranslation('governance.glossary');
    const { t: tc } = useTranslation('common.actions');
    const { t: tm } = useTranslation('misc');
    const location = useLocation();
    const isHomeSelected = location.pathname === PageRoutes.GLOSSARY || location.pathname === `${PageRoutes.GLOSSARY}/`;
    const [isCollapsed, setIsCollapsed] = useState(false);
    const [isCreateNodeModalVisible, setIsCreateNodeModalVisible] = useState(false);
    const [isCreateTermModalVisible, setIsCreateTermModalVisible] = useState(false);
    const [isCreateMenuOpen, setIsCreateMenuOpen] = useState(false);

    const {
        selectedOwnerUrns,
        setSelectedOwnerUrns,
        availableOwners,
        selectedTagUrns,
        setSelectedTagUrns,
        availableTags,
        selectedDomainUrns,
        setSelectedDomainUrns,
        availableDomains,
        sortSelection,
        setSortSelection,
    } = useGlossarySidebarFilters();

    const { data: nodesData, refetch: refetchForNodes } = useGetRootGlossaryNodesQuery();
    const { data: termsData, refetch: refetchForTerms } = useGetRootGlossaryTermsQuery();

    const user = useUserContext();
    const canManageGlossaries = user?.platformPrivileges?.manageGlossaries;

    const entityRegistry = useEntityRegistry();
    const generateColor = useGenerateGlossaryColorFromPalette();
    const { entityData } = useGlossaryEntityData();
    const selectedEntityUrn = entityData?.urn;

    const expandSidebar = useCallback(() => setIsCollapsed(false), []);

    const rootNodes = useMemo(() => nodesData?.getRootGlossaryNodes?.nodes ?? [], [nodesData]);
    const rootTerms = useMemo(() => termsData?.getRootGlossaryTerms?.terms ?? [], [termsData]);

    const collapsedItems = useMemo(
        () =>
            getCollapsedGlossaryItems({
                nodes: rootNodes,
                terms: rootTerms,
                entityRegistry,
                generateColor,
            }),
        [rootNodes, rootTerms, entityRegistry, generateColor],
    );

    const createMenuItems = useMemo<ItemType[]>(
        () => [
            {
                type: 'item',
                key: 'add-term-group',
                title: t('empty.addTermGroup'),
                icon: BookmarksSimple,
                onClick: () => {
                    setIsCreateNodeModalVisible(true);
                    setIsCreateMenuOpen(false);
                },
                dataTestId: 'glossary-sidebar-add-term-group',
            },
            {
                type: 'item',
                key: 'add-term',
                title: t('empty.addTerm'),
                icon: BookmarkSimple,
                onClick: () => {
                    setIsCreateTermModalVisible(true);
                    setIsCreateMenuOpen(false);
                },
                dataTestId: 'glossary-sidebar-add-term',
            },
        ],
        [t],
    );

    const headerActions = (
        <Menu
            open={isCreateMenuOpen}
            onOpenChange={setIsCreateMenuOpen}
            items={createMenuItems}
            trigger={['click']}
            placement="bottomRight"
        >
            <SidebarCreateButton
                variant="filled"
                color="primary"
                isCircle
                icon={{ icon: Plus }}
                data-testid="create-glossary-button"
            />
        </Menu>
    );

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
            { value: GLOSSARY_SIDEBAR_SORT.NAME_ASC, label: tm('sidebarSort.nameAtoZ') },
            { value: GLOSSARY_SIDEBAR_SORT.NAME_DESC, label: tm('sidebarSort.nameZtoA') },
        ],
        [tm],
    );

    const filters = (
        <FiltersRow>
            <SimpleSelect
                size="sm"
                width="fit-content"
                isMultiSelect
                showSearch
                filterResultsByQuery
                isDisabled={ownerOptions.length === 0}
                placeholder={t('sidebar.ownerFilter.placeholder')}
                selectLabelProps={{
                    variant: 'labeled',
                    label: t('sidebar.ownerFilter.label'),
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
                dataTestId="glossary-sidebar-owner-filter"
            />
            <SimpleSelect<FacetSelectOption>
                size="sm"
                width="fit-content"
                isMultiSelect
                showSearch
                filterResultsByQuery
                isDisabled={availableTags.length === 0 && selectedTagUrns.length === 0}
                placeholder={t('sidebar.tagFilter.placeholder')}
                selectLabelProps={{
                    variant: 'labeled',
                    label: t('sidebar.tagFilter.label'),
                }}
                options={availableTags}
                values={selectedTagUrns}
                onUpdate={setSelectedTagUrns}
                renderCustomOptionText={(option) => {
                    if (!isTag(option.entity)) return option.label;
                    return <TagLink tag={option.entity} enableTooltip={false} enableDrawer={false} />;
                }}
                dataTestId="glossary-sidebar-tag-filter"
            />
            <SimpleSelect<FacetSelectOption>
                size="sm"
                width="fit-content"
                isMultiSelect
                showSearch
                filterResultsByQuery
                isDisabled={availableDomains.length === 0 && selectedDomainUrns.length === 0}
                placeholder={t('sidebar.domainFilter.placeholder')}
                selectLabelProps={{
                    variant: 'labeled',
                    label: t('sidebar.domainFilter.label'),
                }}
                options={availableDomains}
                values={selectedDomainUrns}
                onUpdate={setSelectedDomainUrns}
                renderCustomOptionText={(option) => {
                    if (!isDomain(option.entity)) return option.label;
                    return (
                        <DomainLink
                            domain={option.entity}
                            readOnly
                            enableTooltip={false}
                            iconSize={20}
                            iconFontSize={12}
                        />
                    );
                }}
                dataTestId="glossary-sidebar-domain-filter"
            />
        </FiltersRow>
    );

    return (
        <>
            <HierarchicalBrowseSidebar
                title={t('page.title')}
                isCollapsed={isCollapsed}
                onToggleCollapsed={() => setIsCollapsed((prev) => !prev)}
                onExpandSidebar={expandSidebar}
                headerActions={headerActions}
                dataTestId="glossary-browser-sidebar"
                collapseButtonTestId="glossary-sidebar-toggle"
                expandTooltip={t('sidebar.expand')}
                collapseTooltip={t('sidebar.collapse')}
                search={<GlossarySearch />}
                sort={
                    <SidebarSortSelect
                        options={sortOptions}
                        value={sortSelection}
                        onChange={(next) => setSortSelection(next as GlossarySidebarSortValue)}
                        dataTestId="glossary-sidebar-sort"
                    />
                }
                filters={filters}
                homeNav={
                    <SidebarHomeNavLink
                        to={PageRoutes.GLOSSARY}
                        isSelected={isHomeSelected}
                        label={t('sidebar.home')}
                    />
                }
                collapsedIcons={{
                    searchAriaLabel: tc('search'),
                    searchTestId: 'glossary-sidebar-search-icon',
                    home: {
                        to: PageRoutes.GLOSSARY,
                        label: t('sidebar.home'),
                        isSelected: isHomeSelected,
                    },
                    items: collapsedItems.map((item) => ({
                        key: item.urn,
                        to: entityRegistry.getEntityUrl(item.type, item.urn),
                        label: item.name,
                        isSelected: selectedEntityUrn === item.urn,
                        icon: (
                            <GlossaryColoredIcon
                                color={item.color}
                                icon={item.Icon}
                                size={TREE_ROW_ENTITY_ICON_SIZE}
                                iconSize={TREE_ROW_ENTITY_ICON_GLYPH_SIZE}
                            />
                        ),
                    })),
                }}
            >
                <GlossaryBrowser key={sortSelection} openToEntity />
            </HierarchicalBrowseSidebar>
            {isCreateNodeModalVisible && (
                <CreateGlossaryEntityModal
                    entityType={EntityType.GlossaryNode}
                    canCreateGlossaryEntity={!!canManageGlossaries}
                    onClose={() => setIsCreateNodeModalVisible(false)}
                    refetchData={refetchForNodes}
                />
            )}
            {isCreateTermModalVisible && (
                <CreateGlossaryEntityModal
                    entityType={EntityType.GlossaryTerm}
                    canCreateGlossaryEntity={!!canManageGlossaries}
                    onClose={() => setIsCreateTermModalVisible(false)}
                    refetchData={refetchForTerms}
                />
            )}
        </>
    );
}

export default function GlossarySidebar() {
    return (
        <GlossarySidebarFiltersProvider>
            <GlossarySidebarInner />
        </GlossarySidebarFiltersProvider>
    );
}
