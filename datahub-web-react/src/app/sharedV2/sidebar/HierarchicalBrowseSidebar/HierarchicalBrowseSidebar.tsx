import { Button, Tooltip } from '@components';
import { ArrowLineLeft } from '@phosphor-icons/react/dist/csr/ArrowLineLeft';
import { ArrowLineRight } from '@phosphor-icons/react/dist/csr/ArrowLineRight';
import { MagnifyingGlass } from '@phosphor-icons/react/dist/csr/MagnifyingGlass';
import React, { useCallback, useState } from 'react';

import {
    Content,
    FiltersRow,
    HeaderButtons,
    HeaderControls,
    HomeNavSlot,
    SearchIconButton,
    SearchSlot,
    SidebarContainer,
    SidebarTitle,
    ThinDivider,
    TreeContainer,
} from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseSidebar.components';
import SidebarCollapsedIconRail, {
    type SidebarCollapsedIconRailProps,
} from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/SidebarCollapsedIconRail';
import { TREE_ROW_ENTITY_ICON_SIZE } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/constants';
import {
    resolveCollapsedBodyMode,
    shouldPlaceHomeAboveDivider,
} from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/utils/sidebarLayout';
import useSidebarWidth from '@app/sharedV2/sidebar/useSidebarWidth';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';

type BodyRenderProps = {
    isCollapsed: boolean;
};

type Props = {
    title?: React.ReactNode;
    /** Controlled collapse. When omitted, the shell manages its own state. */
    isCollapsed?: boolean;
    onToggleCollapsed?: () => void;
    defaultCollapsed?: boolean;
    /** Expand the sidebar (e.g. from the collapsed search icon). Defaults to expanding. */
    onExpandSidebar?: () => void;
    /**
     * Header actions before the collapse toggle (typically `SidebarCreateButton`).
     * Omit for pages without create (e.g. Metrics).
     */
    headerActions?: React.ReactNode;
    /**
     * Search control only (SearchBar / autocomplete). Shell wraps it in SearchSlot
     * padding — do not add your own 12px InputWrapper. Use `SearchResultsDropdown`
     * for autocomplete results.
     */
    search?: React.ReactNode;
    /**
     * Filter controls. When provided, the shell wraps them in FiltersRow and
     * places home *below* the post-filter divider:
     *   Search → Filters → Divider → Home → Section label…
     * Omit / `null` / `false` for no-filter pages — home sits *above* the divider:
     *   Search → Home → Divider → Section label…
     */
    filters?: React.ReactNode;
    /**
     * Optional home / overview row (`SidebarHomeNavLink`). Placement relative to
     * the body divider depends on whether `filters` are present.
     */
    homeNav?: React.ReactNode;
    /**
     * Tree body — section headers (`TreeSectionHeader`) + rows
     * (`HierarchicalBrowseTreeRow`). Prefer plain children.
     *
     * Use a render function when the same tree must stay mounted across
     * collapse (Domains nested icons) — then omit `collapsedIcons` and branch
     * on `isCollapsed` inside the render prop.
     */
    children: React.ReactNode | ((props: BodyRenderProps) => React.ReactNode);
    /**
     * Standard collapsed icon rail (search + optional home + entity icons).
     * Shell owns chrome; page only supplies the icon data.
     */
    collapsedIcons?: Omit<SidebarCollapsedIconRailProps, 'onExpandSearch'>;
    collapsedSearchAriaLabel?: string;
    collapsedSearchTestId?: string;
    dataTestId?: string;
    id?: string;
    collapseButtonTestId?: string;
    expandTooltip?: string;
    collapseTooltip?: string;
    /** Skip internal width calculation when the parent already owns width. */
    width?: number;
    className?: string;
};

/**
 * Plug-and-play hierarchical browse sidebar.
 *
 * Compose this shell + shared helpers — do not re-implement collapse, padding,
 * dividers, or tree scroll. SaaS: take `HierarchicalBrowseSidebar/**` wholesale;
 * keep fork badges on TermItem via `afterLabel`.
 */
export default function HierarchicalBrowseSidebar({
    title,
    isCollapsed: controlledCollapsed,
    onToggleCollapsed,
    defaultCollapsed = false,
    onExpandSidebar,
    headerActions,
    search,
    filters,
    homeNav,
    children,
    collapsedIcons,
    collapsedSearchAriaLabel,
    collapsedSearchTestId,
    dataTestId,
    id,
    collapseButtonTestId,
    expandTooltip,
    collapseTooltip,
    width: widthOverride,
    className,
}: Props) {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const measuredWidth = useSidebarWidth(0.2);
    const width = widthOverride ?? measuredWidth;

    const [uncontrolledCollapsed, setUncontrolledCollapsed] = useState(defaultCollapsed);
    const isControlled = controlledCollapsed !== undefined;
    const isCollapsed = isControlled ? controlledCollapsed : uncontrolledCollapsed;

    const toggleCollapsed = useCallback(() => {
        if (onToggleCollapsed) {
            onToggleCollapsed();
            return;
        }
        setUncontrolledCollapsed((prev) => !prev);
    }, [onToggleCollapsed]);

    const expandSidebar = useCallback(() => {
        if (onExpandSidebar) {
            onExpandSidebar();
            return;
        }
        if (onToggleCollapsed && isCollapsed) {
            onToggleCollapsed();
            return;
        }
        setUncontrolledCollapsed(false);
    }, [onExpandSidebar, onToggleCollapsed, isCollapsed]);

    const collapseTooltipTitle = isCollapsed ? expandTooltip : collapseTooltip;
    const collapseButton = (
        <Button
            variant="text"
            color="gray"
            size="lg"
            isCircle
            icon={{ icon: isCollapsed ? ArrowLineRight : ArrowLineLeft, color: 'icon' }}
            isActive={!isCollapsed}
            onClick={toggleCollapsed}
            data-testid={collapseButtonTestId}
        />
    );

    const isRenderProp = typeof children === 'function';
    const showFilters = filters != null && filters !== false;
    const placeHomeAboveDivider = shouldPlaceHomeAboveDivider({
        showFilters,
        hasHomeNav: homeNav != null,
    });
    const homeAboveDivider = placeHomeAboveDivider ? <HomeNavSlot>{homeNav}</HomeNavSlot> : null;
    const showDividerBeforeTree = search != null || showFilters || homeAboveDivider != null;

    const collapsedMode = resolveCollapsedBodyMode(collapsedIcons != null);

    let resolvedCollapsedContent: React.ReactNode;
    if (collapsedMode === 'icons' && collapsedIcons != null) {
        resolvedCollapsedContent = (
            <SidebarCollapsedIconRail
                {...collapsedIcons}
                onExpandSearch={expandSidebar}
                searchAriaLabel={collapsedIcons.searchAriaLabel ?? collapsedSearchAriaLabel}
                searchTestId={collapsedIcons.searchTestId ?? collapsedSearchTestId}
            />
        );
    } else {
        resolvedCollapsedContent = (
            <SearchIconButton
                onClick={expandSidebar}
                data-testid={collapsedSearchTestId}
                aria-label={collapsedSearchAriaLabel}
            >
                <MagnifyingGlass size={TREE_ROW_ENTITY_ICON_SIZE} weight="regular" />
            </SearchIconButton>
        );
    }

    const renderTree = (tree: React.ReactNode) => {
        const treeBody =
            showFilters && homeNav != null ? (
                <>
                    <HomeNavSlot $inTree>{homeNav}</HomeNavSlot>
                    {tree}
                </>
            ) : (
                tree
            );
        return <TreeContainer>{treeBody}</TreeContainer>;
    };

    const renderExpandedBody = (tree: React.ReactNode) => (
        <Content>
            {search != null ? <SearchSlot>{search}</SearchSlot> : null}
            {showFilters && <FiltersRow>{filters}</FiltersRow>}
            {homeAboveDivider}
            {showDividerBeforeTree && <ThinDivider />}
            {renderTree(tree)}
        </Content>
    );

    let body: React.ReactNode;
    if (isRenderProp) {
        body = isCollapsed ? (
            <Content>{children({ isCollapsed })}</Content>
        ) : (
            renderExpandedBody(children({ isCollapsed: false }))
        );
    } else if (isCollapsed) {
        body = resolvedCollapsedContent;
    } else {
        body = renderExpandedBody(children);
    }

    return (
        <SidebarContainer
            $isCollapsed={isCollapsed}
            $width={width}
            $isShowNavBarRedesign={isShowNavBarRedesign}
            data-testid={dataTestId}
            id={id}
            className={className}
        >
            <HeaderControls $isCollapsed={isCollapsed}>
                {!isCollapsed && title != null ? <SidebarTitle>{title}</SidebarTitle> : null}
                <HeaderButtons>
                    {!isCollapsed && headerActions}
                    {collapseTooltipTitle ? (
                        <Tooltip title={collapseTooltipTitle} placement="right" showArrow={false}>
                            {collapseButton}
                        </Tooltip>
                    ) : (
                        collapseButton
                    )}
                </HeaderButtons>
            </HeaderControls>
            <ThinDivider />
            {body}
        </SidebarContainer>
    );
}
