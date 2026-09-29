import { Placement, arrow, autoUpdate, flip, offset, shift, useFloating } from '@floating-ui/react-dom';
import React, {
    CSSProperties,
    ReactElement,
    ReactNode,
    Ref,
    useCallback,
    useEffect,
    useLayoutEffect,
    useMemo,
    useRef,
    useState,
} from 'react';
import { createPortal } from 'react-dom';
import styled, { useTheme } from 'styled-components';

export type OverlayPlacement =
    | 'top'
    | 'topLeft'
    | 'topRight'
    | 'bottom'
    | 'bottomLeft'
    | 'bottomRight'
    | 'left'
    | 'leftTop'
    | 'leftBottom'
    | 'right'
    | 'rightTop'
    | 'rightBottom';

export type OverlayTrigger = 'hover' | 'focus' | 'click';

export type FloatingOverlayProps = {
    children?: ReactNode;
    content?: ReactNode | (() => ReactNode);
    placement?: OverlayPlacement;
    trigger?: OverlayTrigger | OverlayTrigger[];
    open?: boolean;
    visible?: boolean;
    defaultOpen?: boolean;
    onOpenChange?: (open: boolean) => void;
    onVisibleChange?: (open: boolean) => void;
    mouseEnterDelay?: number;
    mouseLeaveDelay?: number;
    showArrow?: boolean;
    overlayClassName?: string;
    overlayStyle?: CSSProperties;
    overlayInnerStyle?: CSSProperties;
    className?: string;
    style?: CSSProperties;
    'data-testid'?: string;
    zIndex?: number;
    color?: string;
    align?: {
        offset?: [number, number];
    };
    getPopupContainer?: (triggerNode: HTMLElement) => HTMLElement;
    destroyTooltipOnHide?: boolean | { keepParent?: boolean };
    /** Clamps the overlay content to this many lines. Values below 1 disable clamping. */
    maxLines?: number;
    // antd Trigger clones its child and passes these. They have to reach the DOM node or a
    // dropdown whose trigger is a Tooltip/Popover never opens and never gets positioned.
    onClick?: React.MouseEventHandler<HTMLElement>;
    onMouseDown?: React.MouseEventHandler<HTMLElement>;
    onTouchStart?: React.TouchEventHandler<HTMLElement>;
    onMouseEnter?: React.MouseEventHandler<HTMLElement>;
    onMouseLeave?: React.MouseEventHandler<HTMLElement>;
    onMouseMove?: React.MouseEventHandler<HTMLElement>;
    onFocus?: React.FocusEventHandler<HTMLElement>;
    onBlur?: React.FocusEventHandler<HTMLElement>;
    onContextMenu?: React.MouseEventHandler<HTMLElement>;
};

type FloatingOverlayInternalProps = FloatingOverlayProps & {
    role: 'tooltip' | 'dialog';
    defaultMaxWidth?: number;
};

type ElementWithRef = ReactElement & {
    ref?: Ref<HTMLElement>;
};

const PLACEMENTS: Record<OverlayPlacement, Placement> = {
    top: 'top',
    topLeft: 'top-start',
    topRight: 'top-end',
    bottom: 'bottom',
    bottomLeft: 'bottom-start',
    bottomRight: 'bottom-end',
    left: 'left',
    leftTop: 'left-start',
    leftBottom: 'left-end',
    right: 'right',
    rightTop: 'right-start',
    rightBottom: 'right-end',
};

const DEFAULT_TRIGGERS: OverlayTrigger[] = ['hover', 'focus'];

const ClampedContent = styled.div<{ $maxLines: number }>`
    display: -webkit-box;
    -webkit-box-orient: vertical;
    -webkit-line-clamp: ${(props) => props.$maxLines};
    overflow: hidden;
    overflow-wrap: anywhere;
`;

function resolveContent(content: FloatingOverlayProps['content']): ReactNode {
    return typeof content === 'function' ? content() : content;
}

function hasContent(content: ReactNode): boolean {
    return content !== null && content !== undefined && content !== false && content !== '';
}

const DISABLEABLE_TAGS = new Set(['button', 'input', 'select', 'textarea']);

/**
 * React suppresses mouse events on disabled form controls, mirroring the browser, so an overlay
 * bound directly to one would never open — and explaining *why* a control is disabled is a large
 * part of what these overlays are for. Callers of this get a wrapper element to listen on instead.
 */
function isDisabledControl(element: ReactElement): boolean {
    if (!element.props?.disabled) return false;
    if (typeof element.type === 'string') return DISABLEABLE_TAGS.has(element.type);
    // antd's Button/Switch/Radio tag themselves and render a native control underneath.
    const componentType = element.type as {
        __ANT_BUTTON?: boolean;
        __ANT_SWITCH?: boolean;
        __ANT_RADIO?: boolean;
        displayName?: string;
    };
    if (componentType.__ANT_BUTTON || componentType.__ANT_SWITCH || componentType.__ANT_RADIO) return true;
    // Alchemy Button renders a native <button> and forwards `disabled` onto it.
    return componentType.displayName === 'Button';
}

const FORWARD_REF_TYPE = Symbol.for('react.forward_ref');
const MEMO_TYPE = Symbol.for('react.memo');

/**
 * Whether cloning `ref` onto this element can reach a DOM node. Plain function components drop
 * the ref (and React warns), class components hand back the instance; either way the overlay
 * would have nothing to anchor to and render at the page origin. antd used `findDOMNode` for
 * these; here they get a layout-neutral wrapper to anchor on instead.
 */
function acceptsDomRef(type: ReactElement['type']): boolean {
    if (typeof type === 'string') return true;
    if (typeof type !== 'object' || type === null) return false;
    const composite = type as { $$typeof?: symbol; type?: ReactElement['type'] };
    if (composite.$$typeof === MEMO_TYPE && composite.type) return acceptsDomRef(composite.type);
    return composite.$$typeof === FORWARD_REF_TYPE;
}

function assignRef(ref: Ref<HTMLElement> | undefined, node: HTMLElement | null): void {
    if (typeof ref === 'function') {
        ref(node);
    } else if (ref) {
        const mutableRef = ref as React.MutableRefObject<HTMLElement | null>;
        mutableRef.current = node;
    }
}

const FloatingOverlay = React.forwardRef<HTMLElement, FloatingOverlayInternalProps>(
    (
        {
            children,
            content: contentProp,
            placement = 'top',
            trigger = DEFAULT_TRIGGERS,
            open: controlledOpen,
            visible,
            defaultOpen = false,
            onOpenChange,
            onVisibleChange,
            mouseEnterDelay = 0.1,
            mouseLeaveDelay = 0.1,
            showArrow = false,
            overlayClassName,
            overlayStyle,
            overlayInnerStyle,
            className,
            style,
            'data-testid': dataTestId,
            zIndex = 1050,
            color,
            align,
            getPopupContainer,
            destroyTooltipOnHide = true,
            maxLines,
            onClick: parentOnClick,
            onMouseDown: parentOnMouseDown,
            onTouchStart: parentOnTouchStart,
            onMouseEnter: parentOnMouseEnter,
            onMouseLeave: parentOnMouseLeave,
            onMouseMove: parentOnMouseMove,
            onFocus: parentOnFocus,
            onBlur: parentOnBlur,
            onContextMenu: parentOnContextMenu,
            role,
            defaultMaxWidth,
        },
        forwardedRef,
    ) => {
        const theme = useTheme();
        const arrowRef = useRef<HTMLDivElement>(null);
        const openTimer = useRef<ReturnType<typeof setTimeout>>();
        const closeTimer = useRef<ReturnType<typeof setTimeout>>();
        // Clicking non-focusable content inside the overlay blurs the trigger with a null
        // `relatedTarget`, so the pointer state is tracked separately to keep the overlay open.
        const pointerDownInOverlay = useRef(false);
        // Menus and popups opened from the overlay's content render in their own portals, outside the
        // overlay's DOM node. React still bubbles their events through the overlay, so this flag lets a
        // press inside them count as inside rather than dismissing the overlay and unmounting them.
        const pointerDownInOverlayTree = useRef(false);
        // A `forwardRef` child can still swallow the ref (e.g. `styled()` around a plain function
        // component). That only shows up after commit, so it's tracked here and the child gets
        // re-rendered inside an anchor wrapper.
        const directRefAttempted = useRef(false);
        const [childDroppedRef, setChildDroppedRef] = useState(false);
        const [uncontrolledOpen, setUncontrolledOpen] = useState(defaultOpen);
        const [overlayId] = useState(() => `alchemy-overlay-${Math.random().toString(36).slice(2)}`);
        const isControlled = controlledOpen !== undefined || visible !== undefined;
        const isOpen = controlledOpen ?? visible ?? uncontrolledOpen;
        const content = resolveContent(contentProp);
        const contentPresent = hasContent(content);
        const triggers = useMemo(() => (Array.isArray(trigger) ? trigger : [trigger]), [trigger]);
        const alignmentOffset = align?.offset;

        const setOpen = useCallback(
            (nextOpen: boolean) => {
                if (!isControlled) setUncontrolledOpen(nextOpen);
                onOpenChange?.(nextOpen);
                onVisibleChange?.(nextOpen);
            },
            [isControlled, onOpenChange, onVisibleChange],
        );

        const {
            refs,
            floatingStyles,
            middlewareData,
            placement: resolvedPlacement,
        } = useFloating({
            open: isOpen && hasContent(content),
            placement: PLACEMENTS[placement],
            whileElementsMounted: autoUpdate,
            // Position with top/left rather than transform so callers can pin an edge via
            // `overlayStyle` (e.g. `left: 0` for full-width menus) and keep their offsets.
            transform: false,
            middleware: [
                offset({
                    mainAxis: 8 + (alignmentOffset?.[1] ?? 0),
                    crossAxis: alignmentOffset?.[0] ?? 0,
                }),
                flip({ padding: 8 }),
                shift({ padding: 8 }),
                ...(showArrow ? [arrow({ element: arrowRef })] : []),
            ],
        });

        const clearOpenTimer = useCallback(() => {
            if (openTimer.current) clearTimeout(openTimer.current);
        }, []);
        const clearCloseTimer = useCallback(() => {
            if (closeTimer.current) clearTimeout(closeTimer.current);
        }, []);
        const scheduleOpen = useCallback(() => {
            clearCloseTimer();
            clearOpenTimer();
            openTimer.current = setTimeout(() => setOpen(true), mouseEnterDelay * 1000);
        }, [clearCloseTimer, clearOpenTimer, mouseEnterDelay, setOpen]);
        const scheduleClose = useCallback(() => {
            clearOpenTimer();
            clearCloseTimer();
            closeTimer.current = setTimeout(() => setOpen(false), mouseLeaveDelay * 1000);
        }, [clearCloseTimer, clearOpenTimer, mouseLeaveDelay, setOpen]);

        useEffect(
            () => () => {
                clearOpenTimer();
                clearCloseTimer();
            },
            [clearCloseTimer, clearOpenTimer],
        );

        // Re-checked when the child component or the presence of content changes, since either
        // decides whether a ref was handed out at all.
        const childType = React.isValidElement(children) ? children.type : 'span';
        useLayoutEffect(() => {
            if (directRefAttempted.current && refs.reference.current === null) setChildDroppedRef(true);
        }, [childType, contentPresent, refs.reference]);

        useEffect(() => {
            if (!isOpen) return undefined;

            const handleKeyDown = (event: KeyboardEvent) => {
                if (event.key === 'Escape') setOpen(false);
            };
            const isWithinReference = (node: EventTarget | null) => {
                const referenceElement = refs.reference.current;
                return node instanceof Node && referenceElement instanceof Element && referenceElement.contains(node);
            };
            const isWithinOverlay = (node: EventTarget | null) =>
                node instanceof Node && !!refs.floating.current?.contains(node);

            const handlePointerDown = (event: MouseEvent) => {
                const withinOverlay = isWithinOverlay(event.target) || pointerDownInOverlayTree.current;
                pointerDownInOverlayTree.current = false;
                pointerDownInOverlay.current = withinOverlay;
                if (!triggers.includes('click')) return;
                if (!isWithinReference(event.target) && !withinOverlay) setOpen(false);
            };
            // A press that starts in the overlay may end elsewhere; don't let a stale flag block the next blur.
            const handlePointerUp = () => {
                pointerDownInOverlay.current = false;
                pointerDownInOverlayTree.current = false;
            };
            // Focus leaving the overlay for somewhere other than the trigger closes it, so keyboard users
            // aren't left with an orphaned popover after tabbing through its contents.
            const handleFocusOut = (event: FocusEvent) => {
                if (!triggers.includes('focus')) return;
                const next = event.relatedTarget;
                if (isWithinOverlay(next) || isWithinReference(next)) return;
                setOpen(false);
            };

            const floatingElement = refs.floating.current;
            document.addEventListener('keydown', handleKeyDown);
            document.addEventListener('mousedown', handlePointerDown);
            document.addEventListener('mouseup', handlePointerUp);
            floatingElement?.addEventListener('focusout', handleFocusOut);
            return () => {
                document.removeEventListener('keydown', handleKeyDown);
                document.removeEventListener('mousedown', handlePointerDown);
                document.removeEventListener('mouseup', handlePointerUp);
                floatingElement?.removeEventListener('focusout', handleFocusOut);
            };
        }, [isOpen, refs.floating, refs.reference, setOpen, triggers]);

        // Text or missing children get a synthesized trigger, as antd did, so always-open overlays
        // (e.g. chart series cards rendered inside a positioned container) still have an anchor.
        const child = (
            React.isValidElement(children) ? children : React.createElement('span', undefined, children)
        ) as ElementWithRef;
        const referenceRef = useCallback(
            (node: HTMLElement | null) => {
                refs.setReference(node);
                assignRef(child.ref, node);
                assignRef(forwardedRef, node);
            },
            [child.ref, forwardedRef, refs],
        );
        // The wrapper has no box of its own (`display: contents`), so the overlay anchors to the
        // child's root DOM node inside it. Child refs are called before the parent's, so it exists.
        const anchorWrapperRef = useCallback(
            (node: HTMLElement | null) => {
                const anchor = (node?.firstElementChild as HTMLElement | null) ?? node;
                refs.setReference(anchor);
                assignRef(forwardedRef, anchor);
            },
            [forwardedRef, refs],
        );

        const hasParentHandlers = !!(
            forwardedRef ||
            parentOnClick ||
            parentOnMouseDown ||
            parentOnTouchStart ||
            parentOnMouseEnter ||
            parentOnMouseLeave ||
            parentOnMouseMove ||
            parentOnFocus ||
            parentOnBlur ||
            parentOnContextMenu
        );
        // An empty overlay with nothing to forward can stay the raw child. Cloning it to attach
        // `data-testid={undefined}` overwrites a test id the child sets on its own DOM node.
        directRefAttempted.current = false;
        if (!hasContent(content) && !hasParentHandlers) return child;

        const isWithinOverlay = (node: EventTarget | null): boolean =>
            node instanceof Node && !!refs.floating.current?.contains(node);

        // A disabled control can't host the listeners itself, so they go on a wrapper and the child's
        // own handlers are dropped — matching the browser, which fires nothing for a disabled control.
        const needsDisabledWrapper = isDisabledControl(child);
        // A child that can't take the ref keeps all of its own props; the wrapper only listens.
        const needsAnchorWrapper = !needsDisabledWrapper && (childDroppedRef || !acceptsDomRef(child.type));
        const forwardTo = needsDisabledWrapper || needsAnchorWrapper ? undefined : child.props;
        directRefAttempted.current = !needsDisabledWrapper && !needsAnchorWrapper;

        // Only set an attribute when this overlay has a value for it. `undefined` still overwrites
        // whatever the child renders itself (lineage nodes set `data-testid` inside, then spread props).
        const ariaProps: Record<string, string | boolean> = {};
        if (role === 'tooltip' && isOpen) ariaProps['aria-describedby'] = overlayId;
        if (role === 'dialog' && isOpen) ariaProps['aria-controls'] = overlayId;
        if (role === 'dialog') ariaProps['aria-expanded'] = isOpen;

        const testIdProps = dataTestId ? { 'data-testid': dataTestId } : {};
        const triggerProps = {
            ...ariaProps,
            onMouseEnter: (event: React.MouseEvent<HTMLElement>) => {
                parentOnMouseEnter?.(event);
                forwardTo?.onMouseEnter?.(event);
                if (triggers.includes('hover')) scheduleOpen();
            },
            onMouseLeave: (event: React.MouseEvent<HTMLElement>) => {
                parentOnMouseLeave?.(event);
                forwardTo?.onMouseLeave?.(event);
                if (triggers.includes('hover')) scheduleClose();
            },
            onMouseMove: (event: React.MouseEvent<HTMLElement>) => {
                parentOnMouseMove?.(event);
                forwardTo?.onMouseMove?.(event);
            },
            onMouseDown: (event: React.MouseEvent<HTMLElement>) => {
                parentOnMouseDown?.(event);
                forwardTo?.onMouseDown?.(event);
            },
            onTouchStart: (event: React.TouchEvent<HTMLElement>) => {
                parentOnTouchStart?.(event);
                forwardTo?.onTouchStart?.(event);
            },
            onContextMenu: (event: React.MouseEvent<HTMLElement>) => {
                parentOnContextMenu?.(event);
                forwardTo?.onContextMenu?.(event);
            },
            onFocus: (event: React.FocusEvent<HTMLElement>) => {
                parentOnFocus?.(event);
                forwardTo?.onFocus?.(event);
                if (triggers.includes('focus')) setOpen(true);
            },
            onBlur: (event: React.FocusEvent<HTMLElement>) => {
                parentOnBlur?.(event);
                forwardTo?.onBlur?.(event);
                if (!triggers.includes('focus')) return;
                if (isWithinOverlay(event.relatedTarget) || pointerDownInOverlay.current) return;
                setOpen(false);
            },
            onClick: (event: React.MouseEvent<HTMLElement>) => {
                parentOnClick?.(event);
                forwardTo?.onClick?.(event);
                if (triggers.includes('click')) setOpen(!isOpen);
            },
        };

        let reference: ReactElement;
        if (needsDisabledWrapper) {
            // The child keeps its own className so styled-components styling survives; only the wrapper
            // takes over pointer duties. `inline-block` keeps it from collapsing around the control.
            reference = (
                <span
                    ref={referenceRef}
                    className={className}
                    style={{ display: 'inline-block', cursor: 'not-allowed', ...style }}
                    data-testid={dataTestId}
                    {...triggerProps}
                >
                    {React.cloneElement(child, {
                        style: { ...child.props.style, pointerEvents: 'none' },
                    })}
                </span>
            );
        } else if (needsAnchorWrapper) {
            // React dispatches enter/leave and focus events through the component tree, so the
            // wrapper still hears them even though it contributes no box to layout. The test id
            // stays on the child: a box-less element can't be hovered by browser automation.
            reference = (
                <span ref={anchorWrapperRef} style={{ display: 'contents' }} {...triggerProps}>
                    {React.cloneElement(child, {
                        ...testIdProps,
                        className: [child.props.className, className].filter(Boolean).join(' ') || undefined,
                        style: { ...child.props.style, ...style },
                    })}
                </span>
            );
        } else {
            reference = React.cloneElement(child, {
                ...child.props,
                ref: referenceRef,
                className: [child.props.className, className].filter(Boolean).join(' ') || undefined,
                style: { ...child.props.style, ...style },
                ...testIdProps,
                ...triggerProps,
            });
        }

        const shouldDestroyWhenHidden =
            typeof destroyTooltipOnHide === 'boolean' ? destroyTooltipOnHide : !destroyTooltipOnHide.keepParent;
        if (!hasContent(content) || (!isOpen && shouldDestroyWhenHidden) || typeof document === 'undefined') {
            return reference;
        }

        const triggerNode = refs.reference.current;
        const portalRoot =
            getPopupContainer && triggerNode instanceof HTMLElement ? getPopupContainer(triggerNode) : document.body;
        const backgroundColor = color ?? theme.colors.bgOverlay;
        const isTooltip = role === 'tooltip';
        const side = resolvedPlacement.split('-')[0];
        const staticSide = { top: 'bottom', right: 'left', bottom: 'top', left: 'right' }[side];
        const arrowStyle: CSSProperties = {
            position: 'absolute',
            left: middlewareData.arrow?.x,
            top: middlewareData.arrow?.y,
            width: 8,
            height: 8,
            backgroundColor,
            transform: 'rotate(45deg)',
            ...(staticSide ? { [staticSide]: -4 } : {}),
        };

        // The clamp lives on its own element because `-webkit-line-clamp` requires
        // `display: -webkit-box`, which would override the overlay container's own layout.
        const clampedContent =
            maxLines && maxLines > 0 ? (
                <ClampedContent className="alchemy-floating-overlay-clamp" $maxLines={maxLines}>
                    {content}
                </ClampedContent>
            ) : (
                content
            );

        const overlay = (
            <div
                ref={refs.setFloating}
                id={overlayId}
                role={role}
                className={[overlayClassName, className].filter(Boolean).join(' ') || undefined}
                style={{
                    ...floatingStyles,
                    ...overlayStyle,
                    display: isOpen ? overlayStyle?.display : 'none',
                    zIndex: overlayStyle?.zIndex ?? zIndex,
                }}
                onMouseDownCapture={() => {
                    pointerDownInOverlayTree.current = true;
                }}
                onMouseEnter={clearCloseTimer}
                onMouseLeave={triggers.includes('hover') ? scheduleClose : undefined}
            >
                <div
                    className="alchemy-floating-overlay-inner"
                    style={{
                        maxWidth: defaultMaxWidth,
                        borderRadius: isTooltip ? 8 : 12,
                        backgroundColor,
                        boxShadow: theme.colors.shadowMd,
                        color: isTooltip ? theme.colors.textSecondary : theme.colors.text,
                        fontFamily: 'Mulish',
                        fontSize: 14,
                        lineHeight: '20px',
                        padding: isTooltip ? '4px 8px' : 12,
                        // Keeps content (pills, images, backgrounds) inside the rounded corners.
                        overflow: 'hidden',
                        ...overlayInnerStyle,
                    }}
                >
                    {clampedContent}
                </div>
                {showArrow && <div ref={arrowRef} style={arrowStyle} />}
            </div>
        );

        return (
            <>
                {reference}
                {createPortal(overlay, portalRoot)}
            </>
        );
    },
);

FloatingOverlay.displayName = 'FloatingOverlay';

export default FloatingOverlay;
