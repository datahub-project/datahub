import * as React from 'react';

import FloatingOverlay, {
    FloatingOverlayProps,
    OverlayPlacement,
} from '@components/components/FloatingOverlay/FloatingOverlay';

export type TooltipPlacement = OverlayPlacement;

export type TooltipProps = Omit<FloatingOverlayProps, 'content'> & {
    title?: React.ReactNode | (() => React.ReactNode);
    overlay?: React.ReactNode | (() => React.ReactNode);
};

export const TOOLTIP_MAX_LINES = 5;

const Tooltip = React.forwardRef<HTMLElement, TooltipProps>(
    ({ title, overlay, showArrow = false, maxLines, ...props }, ref) => {
        const content = title ?? overlay;
        // Long descriptions are passed as plain text, so those get clamped by default. Structured
        // content (multi-section panels) is left alone since clamping would cut off whole sections;
        // those callers can opt in by passing maxLines explicitly.
        const isPlainText = typeof content === 'string' || typeof content === 'number';

        return (
            <FloatingOverlay
                ref={ref}
                content={content}
                role="tooltip"
                defaultMaxWidth={320}
                showArrow={showArrow}
                maxLines={maxLines ?? (isPlainText ? TOOLTIP_MAX_LINES : undefined)}
                {...props}
            />
        );
    },
);

Tooltip.displayName = 'Tooltip';

export default Tooltip;
