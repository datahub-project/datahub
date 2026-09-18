import { Placement, arrow, autoUpdate, flip, offset, shift, useFloating } from '@floating-ui/react-dom';
import React, {
    CSSProperties,
    ReactElement,
    ReactNode,
    Ref,
    useCallback,
    useEffect,
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
    };
    return !!(componentType.__ANT_BUTTON || componentType.__ANT_SWITCH || componentType.__ANT_RADIO);
}

function assignRef(ref: Ref<HTMLElement> | undefined, node: HTMLElement | null): void {
    if (typeof ref === 'function') {
        ref(node);
    } else if (ref) {
        const mutableRef = ref as React.MutableRefObject<HTMLElement | null>;
        mutableRef.current = node;
    }
}

export default function FloatingOverlay({
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
    role,
    defaultMaxWidth,
}: FloatingOverlayInternalProps) {
    const theme = useTheme();
    const arrowRef = useRef<HTMLDivElement>(null);
    const openTimer = useRef<ReturnType<typeof setTimeout>>();
    const closeTimer = useRef<ReturnType<typeof setTimeout>>();
    const [uncontrolledOpen, setUncontrolledOpen] = useState(defaultOpen);
    const [overlayId] = useState(() => `alchemy-overlay-${Math.random().toString(36).slice(2)}`);
    const isControlled = controlledOpen !== undefined || visible !== undefined;
    const isOpen = controlledOpen ?? visible ?? uncontrolledOpen;
    const content = resolveContent(contentProp);
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

    useEffect(() => {
        if (!isOpen) return undefined;

        const handleKeyDown = (event: KeyboardEvent) => {
            if (event.key === 'Escape') setOpen(false);
        };
        const handlePointerDown = (event: MouseEvent) => {
            if (!triggers.includes('click')) return;
            const target = event.target as Node;
            const referenceElement = refs.reference.current;
            const isWithinReference = referenceElement instanceof Element && referenceElement.contains(target);
            if (!isWithinReference && !refs.floating.current?.contains(target)) setOpen(false);
        };

        document.addEventListener('keydown', handleKeyDown);
        document.addEventListener('mousedown', handlePointerDown);
        return () => {
            document.removeEventListener('keydown', handleKeyDown);
            document.removeEventListener('mousedown', handlePointerDown);
        };
    }, [isOpen, refs.floating, refs.reference, setOpen, triggers]);

    const child = React.isValidElement(children) ? (children as ElementWithRef) : undefined;
    const referenceRef = useCallback(
        (node: HTMLElement | null) => {
            refs.setReference(node);
            assignRef(child?.ref, node);
        },
        [child?.ref, refs],
    );

    if (!child) return <>{children}</>;
    if (!hasContent(content)) return child;

    // A disabled control can't host the listeners itself, so they go on a wrapper and the child's
    // own handlers are dropped — matching the browser, which fires nothing for a disabled control.
    const needsDisabledWrapper = isDisabledControl(child);
    const forwardTo = needsDisabledWrapper ? undefined : child.props;

    const triggerProps = {
        'aria-describedby': role === 'tooltip' && isOpen ? overlayId : child.props['aria-describedby'],
        'aria-controls': role === 'dialog' && isOpen ? overlayId : child.props['aria-controls'],
        'aria-expanded': role === 'dialog' ? isOpen : child.props['aria-expanded'],
        onMouseEnter: (event: React.MouseEvent<HTMLElement>) => {
            forwardTo?.onMouseEnter?.(event);
            if (triggers.includes('hover')) scheduleOpen();
        },
        onMouseLeave: (event: React.MouseEvent<HTMLElement>) => {
            forwardTo?.onMouseLeave?.(event);
            if (triggers.includes('hover')) scheduleClose();
        },
        onFocus: (event: React.FocusEvent<HTMLElement>) => {
            forwardTo?.onFocus?.(event);
            if (triggers.includes('focus') || triggers.includes('hover')) setOpen(true);
        },
        onBlur: (event: React.FocusEvent<HTMLElement>) => {
            forwardTo?.onBlur?.(event);
            if (triggers.includes('focus') || triggers.includes('hover')) setOpen(false);
        },
        onClick: (event: React.MouseEvent<HTMLElement>) => {
            forwardTo?.onClick?.(event);
            if (triggers.includes('click')) setOpen(!isOpen);
        },
    };

    // The child keeps its own className so styled-components styling survives; only the wrapper
    // takes over pointer duties. `inline-block` keeps it from collapsing around the control.
    const reference = needsDisabledWrapper ? (
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
    ) : (
        React.cloneElement(child, {
            ...child.props,
            ref: referenceRef,
            className: [child.props.className, className].filter(Boolean).join(' ') || undefined,
            style: { ...child.props.style, ...style },
            'data-testid': dataTestId ?? child.props['data-testid'],
            ...triggerProps,
        })
    );

    const shouldDestroyWhenHidden =
        typeof destroyTooltipOnHide === 'boolean' ? destroyTooltipOnHide : !destroyTooltipOnHide.keepParent;
    if ((!isOpen && shouldDestroyWhenHidden) || typeof document === 'undefined') return reference;

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
                ...overlayStyle,
                ...floatingStyles,
                display: isOpen ? overlayStyle?.display : 'none',
                zIndex: overlayStyle?.zIndex ?? zIndex,
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
}
