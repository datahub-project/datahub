import { Button, Menu, Tooltip } from '@components';
import { ArrowLineLeft } from '@phosphor-icons/react/dist/csr/ArrowLineLeft';
import { ArrowLineRight } from '@phosphor-icons/react/dist/csr/ArrowLineRight';
import { BookmarkSimple } from '@phosphor-icons/react/dist/csr/BookmarkSimple';
import { BookmarksSimple } from '@phosphor-icons/react/dist/csr/BookmarksSimple';
import { House } from '@phosphor-icons/react/dist/csr/House';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import React, { useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Link, useLocation } from 'react-router-dom';
import styled from 'styled-components/macro';

import type { ItemType } from '@components/components/Menu/types';

import { useUserContext } from '@app/context/useUserContext';
import CreateGlossaryEntityModal from '@app/entityV2/shared/EntityDropdown/CreateGlossaryEntityModal';
import { useGlossaryEntityData } from '@app/entityV2/shared/GlossaryEntityContext';
import GlossaryBrowser from '@app/glossaryV2/GlossaryBrowser/GlossaryBrowser';
import GlossaryColoredIcon from '@app/glossaryV2/GlossaryColoredIcon';
import GlossarySearch from '@app/glossaryV2/GlossarySearch';
import { useGenerateGlossaryColorFromPalette } from '@app/glossaryV2/colorUtils';
import { getGlossaryEntityIcon } from '@app/glossaryV2/utils';
import useSidebarWidth from '@app/sharedV2/sidebar/useSidebarWidth';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';
import { PageRoutes } from '@conf/Global';

import { useGetRootGlossaryNodesQuery, useGetRootGlossaryTermsQuery } from '@graphql/glossary.generated';
import { EntityType } from '@types';

const SIDEBAR_TRANSITION_MS = 300;
const COLLAPSED_WIDTH = 63;

const SidebarContainer = styled.div<{
    $isCollapsed: boolean;
    $width: number;
    $isShowNavBarRedesign?: boolean;
    $isEntityProfile?: boolean;
}>`
    flex-shrink: 0;
    max-height: 100%;
    width: ${(props) => (props.$isCollapsed ? `${COLLAPSED_WIDTH}px` : `${props.$width}px`)};
    min-width: ${(props) => (props.$isCollapsed ? `${COLLAPSED_WIDTH}px` : `${props.$width}px`)};
    transition:
        width ${SIDEBAR_TRANSITION_MS}ms ease-in-out,
        min-width ${SIDEBAR_TRANSITION_MS}ms ease-in-out;

    background-color: ${(props) => props.theme.colors.bg};
    border-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '8px'};
    display: flex;
    flex-direction: column;
    ${(props) => !props.$isShowNavBarRedesign && 'margin-bottom: 12px;'}
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
            margin: ${props.$isEntityProfile ? '5px 0px 6px 5px' : '0px 4px 0px 0px'};
            box-shadow: ${props.theme.colors.shadowSm};
        `}
    padding-bottom: ${(props) => (props.$isCollapsed ? '0' : '16px')};
`;

const HeaderRow = styled.div<{ $isCollapsed: boolean }>`
    display: flex;
    align-items: center;
    justify-content: ${(props) => (props.$isCollapsed ? 'center' : 'space-between')};
    padding: 12px;
    height: 50px;
    overflow: hidden;
    gap: 12px;
`;

const GlossaryTitle = styled.div`
    font-size: 16px;
    font-weight: bold;
    color: ${(props) => props.theme.colors.text};
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    flex: 1;
    min-width: 0;
`;

const Actions = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    flex-shrink: 0;
`;

const CreateButton = styled(Button)`
    padding: 2px;
    svg {
        width: 20px;
        height: 20px;
    }
`;

const ThinDivider = styled.div`
    height: 1px;
    background: ${(props) => props.theme.colors.border};
`;

const Content = styled.div`
    display: flex;
    flex-direction: column;
    overflow: hidden;
    flex: 1;
`;

// Custom 6px scrollbar — without this, the OS default (~15px on macOS) takes
// space only on the right and pushes the centered icon column visibly left.
// Uses the semantic `scrollbarThumb` / `scrollbarThumbHover` / `scrollbarTrack`
// tokens (gray100 → gray500 on hover) so it stays subtle by default.
const CollapsedList = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    padding: 12px 0;
    gap: 8px;
    overflow-y: auto;
    overflow-x: hidden;
    flex: 1;

    &::-webkit-scrollbar {
        width: 6px;
    }

    &::-webkit-scrollbar-track {
        background: ${(props) => props.theme.colors.scrollbarTrack};
    }

    &::-webkit-scrollbar-thumb {
        background: ${(props) => props.theme.colors.scrollbarThumb};
        border-radius: 3px;
    }

    &::-webkit-scrollbar-thumb:hover {
        background: ${(props) => props.theme.colors.scrollbarThumbHover};
    }

    scrollbar-width: thin;
    scrollbar-color: ${(props) => `${props.theme.colors.scrollbarThumb} ${props.theme.colors.scrollbarTrack}`};
`;

const CollapsedItemLink = styled(Link)<{ $isSelected?: boolean }>`
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 4px;
    border-radius: 8px;
    transition: background-color 0.15s ease;
    background-color: ${(props) => (props.$isSelected ? props.theme.colors.bgActive : 'transparent')};
    color: ${(props) => (props.$isSelected ? props.theme.colors.iconSelected : props.theme.colors.icon)};

    &:hover {
        background-color: ${(props) => props.theme.colors.bgHover};
        color: ${(props) => props.theme.colors.iconHover};
    }
`;

// Matches the tree-row styling used elsewhere in the redesigned sidebars: 38px row with
// a subtle selected background + brand-tinted focus shadow when active, and a neutral
// hover background + neutral focus shadow otherwise. Horizontal margin (12px) matches
// the HeaderRow's `padding: 12px` so the row sits flush with the title + actions above.
// Bottom margin (12px) matches the 12px gutters used by HeaderRow / SearchInputWrapper
// so the gap between this row and the ThinDivider below is visually consistent with
// the rest of the sidebar's spacing rhythm (was 8px, looked too tight).
const HomeNavLink = styled(Link)<{ $isSelected: boolean }>`
    position: relative;
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 4px 8px;
    margin: 0 12px 12px;
    min-height: 38px;
    height: 38px;
    border-radius: 6px;
    text-decoration: none;
    cursor: pointer;
    transition: background-color 0.15s ease;

    ${(props) =>
        props.$isSelected
            ? `
                background: ${props.theme.colors.bgSelectedSubtle};
                box-shadow: ${props.theme.colors.shadowFocusBrand};
            `
            : `
                &:hover {
                    background: ${props.theme.colors.bgHover};
                    box-shadow: ${props.theme.colors.shadowFocus};
                }
            `}
`;

const HomeNavIcon = styled.div<{ $isSelected: boolean }>`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 20px;
    flex-shrink: 0;

    && svg {
        color: ${(props) => (props.$isSelected ? props.theme.colors.iconBrand : props.theme.colors.icon)};
    }
`;

const HomeNavLabel = styled.span<{ $isSelected: boolean }>`
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

type Props = {
    isEntityProfile?: boolean;
};

export default function GlossarySidebar({ isEntityProfile }: Props) {
    const { t } = useTranslation('governance.glossary');
    const location = useLocation();
    // Active when we're on the bare glossary landing page (no specific node/term selected).
    // A trailing slash would still count as the landing page.
    const isHomeSelected = location.pathname === PageRoutes.GLOSSARY || location.pathname === `${PageRoutes.GLOSSARY}/`;
    const [isCreateNodeModalVisible, setIsCreateNodeModalVisible] = useState(false);
    const [isCreateTermModalVisible, setIsCreateTermModalVisible] = useState(false);
    const [isCreateMenuOpen, setIsCreateMenuOpen] = useState(false);
    const [isCollapsed, setIsCollapsed] = useState(false);

    const { data: nodesData, refetch: refetchForNodes } = useGetRootGlossaryNodesQuery();
    const { data: termsData, refetch: refetchForTerms } = useGetRootGlossaryTermsQuery();

    const user = useUserContext();
    const canManageGlossaries = user?.platformPrivileges?.manageGlossaries;

    const width = useSidebarWidth(0.2);
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const entityRegistry = useEntityRegistry();
    const generateColor = useGenerateGlossaryColorFromPalette();
    // Drives the selected-state highlight on collapsed icons so the active
    // term/group is visually identified even with the sidebar collapsed —
    // mirrors how the expanded tree highlights via `entityData?.urn`.
    const { entityData } = useGlossaryEntityData();
    const selectedEntityUrn = entityData?.urn;

    // Stabilize via useMemo so downstream useMemos / passed-prop arrays don't see a fresh `[]`
    // each render — that would defeat memoization in `GlossaryBrowser` and re-fire its
    // `displayedNodes` / `displayedTerms` derivations every tick.
    const rootNodes = useMemo(() => nodesData?.getRootGlossaryNodes?.nodes ?? [], [nodesData]);
    const rootTerms = useMemo(() => termsData?.getRootGlossaryTerms?.terms ?? [], [termsData]);

    // Flatten root nodes + terms into a single icon column for the collapsed sidebar. Sort each
    // section by display name (matches the expanded tree) and concatenate nodes-then-terms so
    // the icon column stays visually grouped by entity type.
    const collapsedItems = useMemo(() => {
        const byDisplayName = <T extends { urn: string; type: EntityType }>(a: T, b: T) =>
            entityRegistry.getDisplayName(a.type, a).localeCompare(entityRegistry.getDisplayName(b.type, b));
        const sortedNodes = [...rootNodes].sort(byDisplayName);
        const sortedTerms = [...rootTerms].sort(byDisplayName);
        return [...sortedNodes, ...sortedTerms].map((entity) => ({
            urn: entity.urn,
            type: entity.type,
            name: entityRegistry.getDisplayName(entity.type, entity),
            // Root-level entries have no parent to inherit from, so the resolver chain reduces
            // to own colorHex (when the entity carries one) → deterministic palette slot
            // seeded by the entity's URN.
            color:
                (entity as { displayProperties?: { colorHex?: string | null } | null }).displayProperties?.colorHex ||
                generateColor(entity.urn),
            Icon: getGlossaryEntityIcon(entity.type),
        }));
    }, [rootNodes, rootTerms, entityRegistry, generateColor]);

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

    return (
        <>
            <SidebarContainer
                $isCollapsed={isCollapsed}
                $width={width}
                data-testid="glossary-browser-sidebar"
                $isShowNavBarRedesign={isShowNavBarRedesign}
                $isEntityProfile={isEntityProfile}
            >
                <HeaderRow $isCollapsed={isCollapsed}>
                    {!isCollapsed && <GlossaryTitle>{t('page.title')}</GlossaryTitle>}
                    <Actions>
                        {!isCollapsed && (
                            <Menu
                                open={isCreateMenuOpen}
                                onOpenChange={setIsCreateMenuOpen}
                                items={createMenuItems}
                                trigger={['click']}
                                placement="bottomRight"
                            >
                                <CreateButton
                                    variant="filled"
                                    color="primary"
                                    isCircle
                                    icon={{ icon: Plus }}
                                    data-testid="create-glossary-button"
                                />
                            </Menu>
                        )}
                        <Tooltip
                            title={isCollapsed ? t('sidebar.expand') : t('sidebar.collapse')}
                            placement="right"
                            showArrow={false}
                        >
                            <Button
                                variant="text"
                                color="gray"
                                size="lg"
                                isCircle
                                icon={{ icon: isCollapsed ? ArrowLineRight : ArrowLineLeft, color: 'icon' }}
                                isActive={!isCollapsed}
                                onClick={() => setIsCollapsed((prev) => !prev)}
                                data-testid="glossary-sidebar-toggle"
                            />
                        </Tooltip>
                    </Actions>
                </HeaderRow>
                <ThinDivider />
                {isCollapsed ? (
                    <CollapsedList>
                        <Tooltip title={t('sidebar.home')} placement="right" showArrow={false}>
                            <CollapsedItemLink to={PageRoutes.GLOSSARY} $isSelected={isHomeSelected}>
                                <House size={20} weight={isHomeSelected ? 'fill' : 'regular'} />
                            </CollapsedItemLink>
                        </Tooltip>
                        {collapsedItems.map((item) => (
                            <Tooltip key={item.urn} title={item.name} placement="right" showArrow={false}>
                                <CollapsedItemLink
                                    to={entityRegistry.getEntityUrl(item.type, item.urn)}
                                    $isSelected={selectedEntityUrn === item.urn}
                                >
                                    <GlossaryColoredIcon color={item.color} icon={item.Icon} size={32} iconSize={16} />
                                </CollapsedItemLink>
                            </Tooltip>
                        ))}
                    </CollapsedList>
                ) : (
                    <Content>
                        <GlossarySearch />
                        <HomeNavLink to={PageRoutes.GLOSSARY} $isSelected={isHomeSelected}>
                            <HomeNavIcon $isSelected={isHomeSelected}>
                                <House size={20} weight={isHomeSelected ? 'fill' : 'regular'} />
                            </HomeNavIcon>
                            <HomeNavLabel $isSelected={isHomeSelected}>{t('sidebar.home')}</HomeNavLabel>
                        </HomeNavLink>
                        <ThinDivider />
                        {/* Pass the already-fetched root data down so `GlossaryBrowser` skips its
                         * own copy of these two queries. The sidebar still owns the queries here
                         * because the create modals call `refetchData={refetchForNodes/Terms}`
                         * once a root entity is created. */}
                        <GlossaryBrowser openToEntity rootNodes={rootNodes} rootTerms={rootTerms} />
                    </Content>
                )}
            </SidebarContainer>
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
