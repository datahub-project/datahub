import React, {
    createContext,
    useCallback,
    useContext,
    useEffect,
    useLayoutEffect,
    useMemo,
    useRef,
    useState,
} from 'react';
import { createPortal } from 'react-dom';
import styled from 'styled-components';

import { TabButtons } from '@components/components/ButtonTabs';
import {
    MenuPlacement,
    PopoverPlacement,
    VIEWPORT_MARGIN_PX,
    ViewportBounds,
    alignWithinViewport,
    placeBelowOrAbove,
    placeBeside,
} from '@components/components/FilterBar/placement';
import useClickOutside from '@components/components/Utils/ClickOutside/useClickOutside';
import { radius, spacing, typography, zIndices } from '@components/theme';

export const Container = styled.div`
    display: flex;
    width: 100%;
    flex-direction: column;
    gap: ${spacing.xsm};
`;

export const GroupContainer = styled.div<{ $nested: boolean }>`
    display: flex;
    flex-direction: column;
    gap: ${spacing.xsm};
    padding: ${(props) => (props.$nested ? spacing.sm : spacing.none)};
    border: ${(props) => (props.$nested ? `1px solid ${props.theme.colors.border}` : 'none')};
    border-radius: ${radius.md};
    background: ${(props) => (props.$nested ? props.theme.colors.bgSurface : 'transparent')};
`;

export const FiltersRow = styled.div`
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: ${spacing.xsm};
`;

export const GroupActions = styled.div`
    display: flex;
    align-items: center;
    gap: ${spacing.xxsm};
`;

/** The all/any switch, framed like a chip so it sits in the rule row. Use with `size="sm"`. */
export const MatchTabs = styled(TabButtons)`
    height: 32px;
    align-items: center;
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: ${radius.md};
`;

/** Shared reset for the inline text affordances: no button chrome, just a hit target. */
const inlineTrigger = `
    border: 0;
    background: transparent;
    font: inherit;
    cursor: pointer;
`;

export const GhostTrigger = styled.button`
    ${inlineTrigger}
    display: inline-flex;
    min-height: 32px;
    align-items: center;
    gap: ${spacing.xxsm};
    padding: 0 ${spacing.xsm};
    border-radius: ${radius.md};
    color: ${(props) => props.theme.colors.textSecondary};
    font-size: ${typography.fontSizes.md};

    &:hover {
        background: ${(props) => props.theme.colors.bgHover};
        color: ${(props) => props.theme.colors.text};
    }
`;

export const Chip = styled.div`
    display: inline-flex;
    min-width: 0;
    max-width: 100%;
    min-height: 32px;
    align-items: center;
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: ${radius.md};
    background: ${(props) => props.theme.colors.bgSurface};
    color: ${(props) => props.theme.colors.text};
    font-size: ${typography.fontSizes.md};
`;

export const FieldName = styled.span`
    flex-shrink: 0;
    padding: 0 ${spacing.xsm};
    white-space: nowrap;
    font-weight: ${typography.fontWeights.semiBold};
`;

export const OperatorText = styled.span`
    display: inline-flex;
    height: 30px;
    align-items: center;
    padding: 0 ${spacing.xsm};
    border-left: 1px solid ${(props) => props.theme.colors.border};
    white-space: nowrap;
`;

export const ChipSegment = styled.button<{ $emphasized?: boolean; $placeholder?: boolean }>`
    ${inlineTrigger}
    display: inline-flex;
    height: 30px;
    min-width: 0;
    max-width: 320px;
    align-items: center;
    gap: ${spacing.xxsm};
    padding: 0 ${spacing.xsm};
    border-left: 1px solid ${(props) => props.theme.colors.border};
    color: ${(props) => (props.$placeholder ? props.theme.colors.textPlaceholder : props.theme.colors.text)};
    font-weight: ${(props) => (props.$emphasized ? typography.fontWeights.semiBold : typography.fontWeights.normal)};
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;

    &:hover {
        background: ${(props) => props.theme.colors.bgHover};
    }
`;

/** Fills the value segment, padding included, so hovering anywhere on it shows the value list. */
export const ValueContent = styled.span`
    display: flex;
    min-width: 0;
    height: 100%;
    align-items: center;
    gap: ${spacing.xxsm};
    margin: 0 -${spacing.xsm};
    padding: 0 ${spacing.xsm};
`;

/** The value's text, which an ellipsis shortens when the chip runs out of room. */
export const ValueText = styled.span`
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
`;

export const ValueList = styled.div`
    display: flex;
    flex-direction: column;
    gap: ${spacing.xxsm};
`;

export const ValueListItem = styled.div`
    display: flex;
    align-items: center;
    gap: ${spacing.xsm};
    color: ${(props) => props.theme.colors.text};
    font-size: ${typography.fontSizes.md};
`;

export const ValueListIcon = styled.span`
    display: inline-flex;
    flex-shrink: 0;
    align-items: center;
`;

/** How many selected values the label leaves out; never shrinks, so the names are cut first. */
export const ValueOverflowCount = styled.span`
    flex-shrink: 0;
`;

export const ValueIconStack = styled.span`
    display: inline-flex;
    flex-shrink: 0;
    align-items: center;
`;

export const ValueIconStackItem = styled.span`
    display: inline-flex;
    align-items: center;
    justify-content: center;
    margin-left: -6px;
    border-radius: ${radius.full};
    background: ${(props) => props.theme.colors.bgSurface};
    box-shadow: 0 0 0 1.5px ${(props) => props.theme.colors.bgSurface};

    &:first-child {
        margin-left: 0;
    }

    > img,
    > svg {
        width: 16px;
        height: 16px;
        border-radius: ${radius.sm};
    }
`;

export const RemoveButton = styled.button`
    ${inlineTrigger}
    display: flex;
    flex-shrink: 0;
    width: 30px;
    height: 30px;
    align-items: center;
    justify-content: center;
    border-left: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 0 ${radius.md} ${radius.md} 0;
    color: ${(props) => props.theme.colors.icon};

    &:hover {
        background: ${(props) => props.theme.colors.bgHover};
        color: ${(props) => props.theme.colors.iconError};
    }
`;

/** A nested group's remove button, at the end of its rule row. */
export const GroupRemoveButton = styled(RemoveButton)`
    margin-left: auto;
    border-left: 0;
    border-radius: ${radius.md};
`;

export const PopoverAnchor = styled.div<{ $shrinkable: boolean }>`
    position: relative;
    display: inline-flex;
    ${(props) => (props.$shrinkable ? 'min-width: 0;' : 'flex-shrink: 0;')}
`;

/**
 * Portaled to document.body with position:fixed so ancestor overflow (search results
 * scroll containers) cannot clip the menu or its side flyouts.
 */
export const PopoverPanel = styled.div<{ $width: number }>`
    position: fixed;
    z-index: ${zIndices.popover};
    display: flex;
    width: ${(props) => props.$width}px;
    max-width: calc(100vw - ${spacing.xlg});
    flex-direction: column;
    gap: ${spacing.xxsm};
    padding: ${spacing.xsm};
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: ${radius.md};
    background: ${(props) => props.theme.colors.bgOverlay};
    box-shadow: ${(props) => props.theme.colors.shadowMd};
    /* Visible so ValueFlyout can extend sideways without being cropped. */
    overflow: visible;
`;

/** Field list + side value flyout for Linear-style Add Filter hover menus. */
export const AddFilterMenu = styled.div`
    position: relative;
    display: flex;
    flex-direction: column;
    gap: ${spacing.xxsm};
    overflow: visible;
`;

/** Gap between a parent menu and its side value/group flyout. */
const FLYOUT_GAP_PX = 8;
const DEFAULT_FLYOUT_WIDTH_PX = 280;

export const ValueFlyout = styled.div<{ $side: 'left' | 'right'; $width?: number }>`
    position: absolute;
    z-index: ${zIndices.popover};
    top: var(--filter-flyout-top, 0);
    bottom: var(--filter-flyout-bottom, auto);
    ${(props) =>
        props.$side === 'right'
            ? `
        left: calc(100% + ${FLYOUT_GAP_PX}px);
        right: auto;
    `
            : `
        right: calc(100% + ${FLYOUT_GAP_PX}px);
        left: auto;
    `}
    display: flex;
    width: ${(props) => props.$width ?? DEFAULT_FLYOUT_WIDTH_PX}px;
    max-height: var(--filter-flyout-max-height, var(--filter-menu-max-height, 480px));
    flex-direction: column;
    gap: ${spacing.xxsm};
    padding: ${spacing.xsm};
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: ${radius.md};
    background: ${(props) => props.theme.colors.bgOverlay};
    box-shadow: ${(props) => props.theme.colors.shadowMd};
    /* Visible so nested value flyouts can extend sideways without being cropped. */
    overflow: visible;
`;

/** Wraps a value picker for test ids without adding a layout box. */
export const ValueEditorPanel = styled.div`
    display: contents;
`;

export const OptionList = styled.div`
    display: flex;
    max-height: var(--filter-menu-max-height, 480px);
    flex-direction: column;
    overflow-x: hidden;
    overflow-y: auto;
`;

export const OptionRow = styled.button<{ $active?: boolean }>`
    ${inlineTrigger}
    display: flex;
    width: 100%;
    min-height: 32px;
    flex-shrink: 0;
    align-items: center;
    gap: ${spacing.xsm};
    padding: ${spacing.xxsm} ${spacing.xsm};
    border-radius: ${radius.sm};
    background: ${(props) => (props.$active ? props.theme.colors.bgHover : 'transparent')};
    color: ${(props) => props.theme.colors.text};
    font-size: ${typography.fontSizes.md};
    text-align: left;

    &:hover:not(:disabled) {
        background: ${(props) => props.theme.colors.bgHover};
    }

    &:disabled {
        color: ${(props) => props.theme.colors.textDisabled};
        cursor: not-allowed;
    }
`;

export const OptionContent = styled.span`
    display: flex;
    min-width: 0;
    flex: 1;
    flex-direction: column;
`;

export const OptionLabel = styled.span`
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

export const OptionDescription = styled.span`
    overflow: hidden;
    color: ${(props) => props.theme.colors.textSecondary};
    font-size: ${typography.fontSizes.sm};
    text-overflow: ellipsis;
    white-space: nowrap;
`;

export const OptionCount = styled.span`
    color: ${(props) => props.theme.colors.textSecondary};
    font-size: ${typography.fontSizes.sm};
`;

export const OptionCountLoader = styled.span`
    display: inline-flex;
    align-items: center;
`;

export const NestedOptionIndent = styled.span<{ $depth: number }>`
    display: ${(props) => (props.$depth > 0 ? 'inline-block' : 'none')};
    width: ${(props) => props.$depth * 16}px;
    flex-shrink: 0;
`;

export const ExpandToggle = styled.button`
    ${inlineTrigger}
    display: inline-flex;
    width: 22px;
    height: 22px;
    flex-shrink: 0;
    align-items: center;
    justify-content: center;
    color: ${(props) => props.theme.colors.icon};
    border-radius: ${radius.sm};

    &:hover {
        background: ${(props) => props.theme.colors.bgHover};
    }

    svg {
        width: 14px;
        height: 14px;
        min-width: 14px;
        min-height: 14px;
    }
`;

export const ExpandToggleSpacer = styled.span`
    display: inline-block;
    width: 22px;
    height: 22px;
    flex-shrink: 0;
`;

export const OptionCheckboxSlot = styled.span`
    display: inline-flex;
    width: 20px;
    flex-shrink: 0;
    align-items: center;
    justify-content: center;
`;

export const MenuState = styled.div`
    display: flex;
    min-height: 40px;
    align-items: center;
    justify-content: center;
    color: ${(props) => props.theme.colors.textSecondary};
    font-size: ${typography.fontSizes.sm};
`;

export const SectionLabel = styled.div`
    flex-shrink: 0;
    padding: ${spacing.xsm} ${spacing.xsm} ${spacing.xxsm};
    color: ${(props) => props.theme.colors.textSecondary};
    font-size: ${typography.fontSizes.sm};
    font-weight: ${typography.fontWeights.semiBold};
`;

type FilterPopoverProps = {
    isOpen: boolean;
    onClose: () => void;
    trigger: React.ReactNode;
    width?: number;
    /** Lets the trigger shrink, e.g. a chip's value, so the chip fits its row. */
    shrinkable?: boolean;
};

/** Leave room for the search input above the scrollable option list. */
const OPTION_LIST_CHROME_PX = 52;

/** Where every menu of a filter bar opens; set once by the bar so its chips and Add filter menus agree. */
export const MenuPlacementContext = createContext<MenuPlacement>('bottom');
/** The element menus placed to the right open beside, instead of beside their own trigger. */
export const MenuAnchorContext = createContext<React.RefObject<HTMLElement | null> | undefined>(undefined);

/** Told when each menu opens, and again when it closes or unmounts. */
export const MenuOpenContext = createContext<((open: boolean) => void) | undefined>(undefined);

function getViewportBounds(): ViewportBounds {
    const { offsetTop, offsetLeft, height, width } = getViewportMetrics();
    return { top: offsetTop, bottom: offsetTop + height, left: offsetLeft, right: offsetLeft + width };
}

function getViewportMetrics() {
    const { visualViewport } = window;
    return {
        width: visualViewport?.width ?? window.innerWidth,
        height: visualViewport?.height ?? window.innerHeight,
        offsetTop: visualViewport?.offsetTop ?? 0,
        offsetLeft: visualViewport?.offsetLeft ?? 0,
    };
}

/** Prefer the side with enough room for the flyout; fall back to whichever is larger. */
function pickFlyoutSide(parentRect: DOMRect, flyoutWidth: number): 'left' | 'right' {
    const viewport = getViewportMetrics();
    const needed = flyoutWidth + FLYOUT_GAP_PX;
    const spaceRight = viewport.offsetLeft + viewport.width - parentRect.right - VIEWPORT_MARGIN_PX;
    const spaceLeft = parentRect.left - viewport.offsetLeft - VIEWPORT_MARGIN_PX;
    if (spaceRight >= needed) return 'right';
    if (spaceLeft >= needed) return 'left';
    return spaceRight >= spaceLeft ? 'right' : 'left';
}

/** Marks the menu row a flyout opens from, so the flyout lines up with it. */
export const FLYOUT_ANCHOR_ATTRIBUTE = 'data-flyout-anchor';

function findAnchorRow(parent: HTMLElement, anchorKey: string): HTMLElement | undefined {
    return Array.from(parent.querySelectorAll<HTMLElement>(`[${FLYOUT_ANCHOR_ATTRIBUTE}]`)).find(
        (row) => row.getAttribute(FLYOUT_ANCHOR_ATTRIBUTE) === anchorKey,
    );
}

type ValueFlyoutPanelProps = {
    /** The `FLYOUT_ANCHOR_ATTRIBUTE` value of the row that opened the flyout. */
    anchorKey?: string;
    'aria-label'?: string;
    'data-testid'?: string;
    width?: number;
    children: React.ReactNode;
};

/**
 * Side panel that opens beside its positioned parent, level with the row that opened it. Chooses left vs right from
 * available viewport space so Add Filter near the left edge opens values to the
 * right, and near the right edge opens to the left.
 */
export function ValueFlyoutPanel({
    anchorKey,
    children,
    width = DEFAULT_FLYOUT_WIDTH_PX,
    ...rest
}: ValueFlyoutPanelProps): JSX.Element {
    const ref = useRef<HTMLDivElement>(null);
    const [side, setSide] = useState<'left' | 'right'>('right');
    const [top, setTop] = useState<number | undefined>(undefined);

    useLayoutEffect(() => {
        const el = ref.current;
        const parent = el?.offsetParent instanceof HTMLElement ? el.offsetParent : (el?.parentElement ?? null);
        if (!el || !parent) return undefined;

        const update = () => {
            const parentRect = parent.getBoundingClientRect();
            setSide(pickFlyoutSide(parentRect, width));
            const row = anchorKey ? findAnchorRow(parent, anchorKey) : undefined;
            const rowTop = row ? row.getBoundingClientRect().top : parentRect.top;
            setTop(alignWithinViewport(rowTop, el.offsetHeight, getViewportBounds()) - parentRect.top);
        };

        update();
        const resizeObserver = typeof ResizeObserver === 'undefined' ? undefined : new ResizeObserver(update);
        resizeObserver?.observe(el);
        resizeObserver?.observe(parent);
        window.addEventListener('resize', update);
        window.addEventListener('scroll', update, true);
        window.visualViewport?.addEventListener('resize', update);
        window.visualViewport?.addEventListener('scroll', update);

        return () => {
            resizeObserver?.disconnect();
            window.removeEventListener('resize', update);
            window.removeEventListener('scroll', update, true);
            window.visualViewport?.removeEventListener('resize', update);
            window.visualViewport?.removeEventListener('scroll', update);
        };
    }, [width, anchorKey]);

    return (
        <ValueFlyout
            ref={ref}
            $side={side}
            $width={width}
            style={top === undefined ? undefined : { top, bottom: 'auto' }}
            // eslint-disable-next-line i18next/no-literal-string -- ARIA role, not UI copy
            role="dialog"
            {...rest}
        >
            {children}
        </ValueFlyout>
    );
}

/**
 * Fixed-position placement from the trigger. Measuring available space around the
 * anchor is required — vh/dvh alone ignore where the menu opens on the page.
 */
function usePopoverPlacement(
    anchorRef: React.RefObject<HTMLElement | null>,
    panelRef: React.RefObject<HTMLElement | null>,
    isOpen: boolean,
    menuPlacement: MenuPlacement,
    menuAnchorRef: React.RefObject<HTMLElement | null> | undefined,
    width: number,
): PopoverPlacement | null {
    const [placement, setPlacement] = useState<PopoverPlacement | null>(null);
    const isPlaced = !!placement;

    const update = useCallback(() => {
        const anchor = anchorRef.current;
        if (!anchor) return;
        const rect = anchor.getBoundingClientRect();
        const viewport = getViewportBounds();
        const next =
            menuPlacement === 'right'
                ? placeBeside(
                      rect,
                      viewport,
                      { width, height: panelRef.current?.offsetHeight ?? 0 },
                      menuAnchorRef?.current?.getBoundingClientRect(),
                  )
                : placeBelowOrAbove(rect, viewport, width);

        setPlacement((current) => {
            if (
                current &&
                current.top === next.top &&
                current.bottom === next.bottom &&
                current.left === next.left &&
                current.maxHeight === next.maxHeight
            ) {
                return current;
            }
            return next;
        });
    }, [anchorRef, panelRef, menuPlacement, menuAnchorRef, width]);

    useLayoutEffect(() => {
        if (!isOpen) {
            setPlacement(null);
            return undefined;
        }

        update();
        window.addEventListener('resize', update);
        // Capture so overflow scroll parents (search results) reposition the fixed menu.
        // Equality check above prevents OptionList scroll from re-rendering.
        window.addEventListener('scroll', update, true);
        window.visualViewport?.addEventListener('resize', update);
        window.visualViewport?.addEventListener('scroll', update);

        return () => {
            window.removeEventListener('resize', update);
            window.removeEventListener('scroll', update, true);
            window.visualViewport?.removeEventListener('resize', update);
            window.visualViewport?.removeEventListener('scroll', update);
        };
    }, [isOpen, update]);

    // A menu centred on its trigger moves whenever its height changes, e.g. as values load
    useLayoutEffect(() => {
        const panel = panelRef.current;
        if (menuPlacement !== 'right' || !isPlaced || !panel) return undefined;
        update();
        if (typeof ResizeObserver === 'undefined') return undefined;
        const resizeObserver = new ResizeObserver(update);
        resizeObserver.observe(panel);
        return () => resizeObserver.disconnect();
    }, [menuPlacement, isPlaced, panelRef, update]);

    return placement;
}

/**
 * Anchored overlay used by every menu in the filter bar. Kept local so the bar stays
 * free of third party overlay libraries and inherits alchemy tokens directly.
 */
export function FilterPopover({
    isOpen,
    onClose,
    trigger,
    width = 280,
    shrinkable = false,
    children,
}: React.PropsWithChildren<FilterPopoverProps>): JSX.Element {
    const anchorRef = useRef<HTMLDivElement>(null);
    const panelRef = useRef<HTMLDivElement>(null);
    const menuPlacement = useContext(MenuPlacementContext);
    const menuAnchorRef = useContext(MenuAnchorContext);
    const reportMenuOpen = useContext(MenuOpenContext);
    const placement = usePopoverPlacement(anchorRef, panelRef, isOpen, menuPlacement, menuAnchorRef, width);
    const clickOutsideOptions = useMemo(() => ({ wrappers: [anchorRef, panelRef] }), []);
    const onClickOutside = useCallback(() => {
        if (isOpen) onClose();
    }, [isOpen, onClose]);

    useClickOutside(onClickOutside, clickOutsideOptions);

    useEffect(() => {
        if (!isOpen || !reportMenuOpen) return undefined;
        reportMenuOpen(true);
        return () => reportMenuOpen(false);
    }, [isOpen, reportMenuOpen]);

    useEffect(() => {
        if (!isOpen) return undefined;
        const onKeyDown = (event: KeyboardEvent) => {
            if (event.key === 'Escape') onClose();
        };
        document.addEventListener('keydown', onKeyDown);
        return () => document.removeEventListener('keydown', onKeyDown);
    }, [isOpen, onClose]);

    const optionListMaxHeight = placement ? Math.max(120, placement.maxHeight - OPTION_LIST_CHROME_PX) : undefined;

    return (
        <PopoverAnchor ref={anchorRef} $shrinkable={shrinkable}>
            {trigger}
            {isOpen &&
                placement &&
                createPortal(
                    <PopoverPanel
                        ref={panelRef}
                        $width={width}
                        style={
                            {
                                top: placement.top,
                                bottom: placement.bottom,
                                left: placement.left,
                                '--filter-menu-max-height': `${optionListMaxHeight}px`,
                                ...(menuPlacement === 'right' && {
                                    maxHeight: placement.maxHeight,
                                    '--filter-flyout-max-height': `${placement.maxHeight}px`,
                                }),
                                // Side flyouts line up with the edge nearest the trigger
                                ...(placement.bottom !== undefined && {
                                    '--filter-flyout-top': 'auto',
                                    '--filter-flyout-bottom': '0',
                                }),
                            } as React.CSSProperties
                        }
                    >
                        {children}
                    </PopoverPanel>,
                    document.body,
                )}
        </PopoverAnchor>
    );
}
