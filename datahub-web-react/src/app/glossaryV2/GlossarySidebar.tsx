import { Menu } from '@components';
import { BookmarkSimple } from '@phosphor-icons/react/dist/csr/BookmarkSimple';
import { BookmarksSimple } from '@phosphor-icons/react/dist/csr/BookmarksSimple';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import React, { useCallback, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useLocation } from 'react-router-dom';

import type { ItemType } from '@components/components/Menu/types';

import { useUserContext } from '@app/context/useUserContext';
import CreateGlossaryEntityModal from '@app/entityV2/shared/EntityDropdown/CreateGlossaryEntityModal';
import { useGlossaryEntityData } from '@app/entityV2/shared/GlossaryEntityContext';
import GlossaryBrowser from '@app/glossaryV2/GlossaryBrowser/GlossaryBrowser';
import GlossaryColoredIcon from '@app/glossaryV2/GlossaryColoredIcon';
import GlossarySearch from '@app/glossaryV2/GlossarySearch';
import { useGenerateGlossaryColorFromPalette } from '@app/glossaryV2/colorUtils';
import { getCollapsedGlossaryItems } from '@app/glossaryV2/utils';
import HierarchicalBrowseSidebar from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseSidebar';
import { SidebarCreateButton } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseSidebar.components';
import SidebarHomeNavLink from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/SidebarHomeNavLink';
import {
    TREE_ROW_ENTITY_ICON_GLYPH_SIZE,
    TREE_ROW_ENTITY_ICON_SIZE,
} from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/constants';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { PageRoutes } from '@conf/Global';

import { useGetRootGlossaryNodesQuery, useGetRootGlossaryTermsQuery } from '@graphql/glossary.generated';
import { EntityType } from '@types';

export default function GlossarySidebar() {
    const { t } = useTranslation('governance.glossary');
    const { t: tc } = useTranslation('common.actions');
    const location = useLocation();
    // Active when we're on the bare glossary landing page (no specific node/term selected).
    // A trailing slash would still count as the landing page.
    const isHomeSelected = location.pathname === PageRoutes.GLOSSARY || location.pathname === `${PageRoutes.GLOSSARY}/`;
    const [isCollapsed, setIsCollapsed] = useState(false);
    const [isCreateNodeModalVisible, setIsCreateNodeModalVisible] = useState(false);
    const [isCreateTermModalVisible, setIsCreateTermModalVisible] = useState(false);
    const [isCreateMenuOpen, setIsCreateMenuOpen] = useState(false);

    const { data: nodesData, refetch: refetchForNodes } = useGetRootGlossaryNodesQuery();
    const { data: termsData, refetch: refetchForTerms } = useGetRootGlossaryTermsQuery();

    const user = useUserContext();
    const canManageGlossaries = user?.platformPrivileges?.manageGlossaries;

    const entityRegistry = useEntityRegistry();
    const generateColor = useGenerateGlossaryColorFromPalette();
    // Drives the selected-state highlight on collapsed icons so the active
    // term/group is visually identified even with the sidebar collapsed —
    // mirrors how the expanded tree highlights via `entityData?.urn`.
    const { entityData } = useGlossaryEntityData();
    const selectedEntityUrn = entityData?.urn;

    const expandSidebar = useCallback(() => setIsCollapsed(false), []);

    // Stabilize via useMemo so downstream useMemos / passed-prop arrays don't see a fresh `[]`
    // each render — that would defeat memoization in `GlossaryBrowser` and re-fire its
    // `displayedNodes` / `displayedTerms` derivations every tick.
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

    // Sidebar "+" opens a dropdown that lets users pick Term Group vs Term, since DataHub
    // allows root-level terms and the single-button shortcut hid that workflow. Icons match
    // the entity types as they appear in the sidebar list (BookmarksSimple for groups,
    // BookmarkSimple for terms).
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
                // No `filters` prop — Glossary has no filter band; layout collapses that section.
            >
                {/* Pass the already-fetched root data down so `GlossaryBrowser` skips its
                 * own copy of these two queries. The sidebar still owns the queries here
                 * because the create modals call `refetchData={refetchForNodes/Terms}`
                 * once a root entity is created. */}
                <GlossaryBrowser openToEntity rootNodes={rootNodes} rootTerms={rootTerms} />
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
