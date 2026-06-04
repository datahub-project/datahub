import { Button, Dropdown } from '@components';
import { ArrowLineLeft } from '@phosphor-icons/react/dist/csr/ArrowLineLeft';
import { ArrowLineRight } from '@phosphor-icons/react/dist/csr/ArrowLineRight';
import { Check } from '@phosphor-icons/react/dist/csr/Check';
import { Divider, MenuProps, Typography } from 'antd';
import React, { useCallback, useState } from 'react';
import styled from 'styled-components';

import { SEARCH_RESULTS_BROWSE_SIDEBAR_ID } from '@app/onboarding/config/SearchOnboardingConfig';
import { useIsPlatformBrowseMode } from '@app/searchV2/sidebar/BrowseContext';
import {
    BrowseSortOrder,
    BrowseSortProvider,
    useBrowseSortOrder,
    useSetBrowseSortOrder,
} from '@app/searchV2/sidebar/BrowseSortContext';
import EntityBrowse from '@app/searchV2/sidebar/EntityBrowse';
import PlatformBrowse from '@app/searchV2/sidebar/PlatformBrowse';
import { ProfileSidebarResizer } from '@src/app/entityV2/shared/containers/profile/sidebar/ProfileSidebarResizer';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';

const PLATFORM_BROWSE_TRANSITION_MS = 200;
const MAX_BROWSER_WIDTH = 500;
const MIN_BROWSWER_WIDTH = 260;

const StyledEntitySidebarContainer = styled.div<{
    isCollapsed: boolean;
    isHidden: boolean;
    $width?: number;
    backgroundColor?: string;
    $isShowNavBarRedesign?: boolean;
}>`
    flex: 1;
    overflow: hidden;
    display: ${(props) => (props.isHidden ? 'none' : undefined)};

    ${(props) => !props.isCollapsed && props.$width && `max-width: ${props.$width}px;`}
    ${(props) => props.isCollapsed && 'min-width: 63px; max-width: 63px;'}
 &::-webkit-scrollbar {
        display: none;
    }

    margin: ${(props) => {
        if (props.$isShowNavBarRedesign) {
            return props.isCollapsed ? '8px 4px 5px 5px' : '8px 0px 5px 5px';
        }
        return props.isCollapsed ? '12px' : '12px 0 12px 12px';
    }};
    transition:
        max-width ${PLATFORM_BROWSE_TRANSITION_MS}ms ease-in-out,
        min-width ${PLATFORM_BROWSE_TRANSITION_MS}ms ease-in-out;

    background-color: ${(props) => props.theme.colors.bg};
    border-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '8px'};
    box-shadow: ${(props) => (props.$isShowNavBarRedesign ? props.theme.colors.shadowSm : props.theme.colors.shadowXs)};
`;

const StyledSidebar = styled.div`
    overflow: auto;
    height: 100%;
    display: flex;
    flex-direction: column;
`;

const Controls = styled.div<{ isCollapsed: boolean }>`
    display: flex;
    align-items: center;
    justify-content: ${(props) => (props.isCollapsed ? 'center' : 'flex-start')};
    gap: ${(props) => (props.isCollapsed ? '0' : '8px')};
    height: 52px;
    padding: 16px 12px;
    overflow: hidden;
`;

const HeaderActions = styled.div`
    display: flex;
    align-items: center;
    margin-left: auto;
`;

const NavigateTitle = styled(Typography.Title)<{ isClosed: boolean }>`
    && {
        padding: 0;
        margin: 0;
        min-width: 140px;
        display: ${(props) => (props.isClosed ? 'none' : 'block')};
        font-size: 14px;
        line-height: 20px;
        font-weight: bold;
        color: ${(props) => props.theme.colors.textSecondary};
    }
`;

const SortButton = styled.button<{ $isOpen: boolean }>`
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 36px;
    height: 36px;
    padding: 0;
    border: none;
    border-radius: 999px;
    background-color: ${(props) => (props.$isOpen ? props.theme.colors.bgSurface : 'transparent')};
    color: ${(props) => props.theme.colors.icon};
    cursor: pointer;

    &:hover {
        background-color: ${(props) => props.theme.colors.bgSurface};
    }

    &:focus-visible {
        outline: 2px solid ${(props) => props.theme.colors.primary};
        outline-offset: 2px;
    }
`;

const SortTrigger = styled.span`
    display: inline-flex;
    align-items: center;
    justify-content: center;
`;

const MenuLabel = styled.span`
    display: inline-flex;
    align-items: center;
    justify-content: space-between;
    width: 100%;
    gap: 12px;
`;

const ThinDivider = styled(Divider)`
    margin: 0px;
    padding: 0px;
`;

const SidebarBody = styled.div`
    height: calc(100% - 53px);
    overflow: auto;
    white-space: nowrap;
    /* Hide scrollbar altoghether for Chrome, Safari, and Opera */
    &::-webkit-scrollbar {
        display: none;
    }
    scrollbar-gutter: stable;
    -moz-scrollbar-gutter: stable;
    -webkit-scrollbar-gutter: stable;
`;

type Props = {
    visible: boolean;
};

const SORT_MENU_ITEMS: Array<{ key: BrowseSortOrder; label: string }> = [
    { key: BrowseSortOrder.ALPHABETICAL_ASC, label: 'Alphabetical (A-Z)' },
    { key: BrowseSortOrder.ALPHABETICAL_DESC, label: 'Alphabetical (Z-A)' },
    { key: BrowseSortOrder.RECENTLY_USED, label: 'Recently Used' },
];

const SortIcon = () => (
    <svg width="14" height="14" viewBox="0 0 14 14" aria-hidden="true" fill="none">
        <path d="M2 3h10" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
        <path d="M2 7h7" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
        <path d="M2 11h4" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
    </svg>
);

const BrowseSidebarControls = ({ isClosed, onToggleCollapse }: { isClosed: boolean; onToggleCollapse: () => void }) => {
    const sortOrder = useBrowseSortOrder();
    const setSortOrder = useSetBrowseSortOrder();
    const [isSortMenuOpen, setIsSortMenuOpen] = useState(false);

    const sortMenu: MenuProps = {
        selectable: true,
        selectedKeys: [sortOrder],
        onClick: ({ key }) => {
            setSortOrder(key as BrowseSortOrder);
            setIsSortMenuOpen(false);
        },
        items: SORT_MENU_ITEMS.map((item) => ({
            key: item.key,
            label: (
                <MenuLabel>
                    <span>{item.label}</span>
                    {sortOrder === item.key ? <Check size={14} weight="bold" /> : null}
                </MenuLabel>
            ),
        })),
    };

    return (
        <Controls isCollapsed={isClosed}>
            {!isClosed ? (
                <NavigateTitle level={5} isClosed={isClosed}>
                    Navigate
                </NavigateTitle>
            ) : null}
            {!isClosed ? (
                <Dropdown
                    open={isSortMenuOpen}
                    onOpenChange={setIsSortMenuOpen}
                    menu={sortMenu}
                    placement="bottomRight"
                >
                    <SortTrigger>
                        <SortButton
                            type="button"
                            aria-label="Change browse sort order"
                            aria-haspopup="menu"
                            aria-expanded={isSortMenuOpen}
                            data-testid="browse-sort-toggle"
                            $isOpen={isSortMenuOpen}
                        >
                            <SortIcon />
                        </SortButton>
                    </SortTrigger>
                </Dropdown>
            ) : null}
            <HeaderActions>
                <Button
                    variant="text"
                    color="gray"
                    size="lg"
                    isCircle
                    icon={{ icon: isClosed ? ArrowLineRight : ArrowLineLeft }}
                    isActive={!isClosed}
                    onClick={onToggleCollapse}
                />
            </HeaderActions>
        </Controls>
    );
};

const BrowseSidebarContent = ({ visible }: Props) => {
    const isPlatformBrowseMode = useIsPlatformBrowseMode();
    const [isClosed, setIsClosed] = useState(false);
    const [isHidden, setIsHidden] = useState(false);
    const isShowNavBarRedesign = useShowNavBarRedesign();

    const [sidebarWidth, setSidebarWidth] = useState(MIN_BROWSWER_WIDTH);

    const hideSidebar = useCallback(() => {
        setIsHidden(true);
        setIsClosed(true);
    }, []);

    const unhideSidebar = useCallback(() => setIsHidden(false), []);

    return (
        <>
            <StyledEntitySidebarContainer
                isCollapsed={isClosed}
                isHidden={isHidden}
                $width={sidebarWidth}
                $isShowNavBarRedesign={isShowNavBarRedesign}
                id="browse-v2"
                data-testid="browse-v2-results"
            >
                <BrowseSidebarControls isClosed={isClosed} onToggleCollapse={() => setIsClosed(!isClosed)} />
                <StyledSidebar id={SEARCH_RESULTS_BROWSE_SIDEBAR_ID}>
                    <ThinDivider />
                    <SidebarBody>
                        {!isPlatformBrowseMode ? (
                            <EntityBrowse visible={visible} />
                        ) : (
                            <PlatformBrowse
                                collapsed={isClosed}
                                expand={() => setIsClosed(false)}
                                visible={visible}
                                hideSidebar={hideSidebar}
                                unhideSidebar={unhideSidebar}
                            />
                        )}
                    </SidebarBody>
                </StyledSidebar>
            </StyledEntitySidebarContainer>
            {!isClosed && (
                <ProfileSidebarResizer
                    setSidePanelWidth={(widthProp) => {
                        setSidebarWidth(Math.min(Math.max(widthProp, MIN_BROWSWER_WIDTH), MAX_BROWSER_WIDTH));
                    }}
                    initialSize={sidebarWidth}
                    isSidebarOnLeft
                />
            )}
        </>
    );
};

const BrowseSidebar = ({ visible }: Props) => {
    return (
        <BrowseSortProvider>
            <BrowseSidebarContent visible={visible} />
        </BrowseSortProvider>
    );
};

export default BrowseSidebar;
