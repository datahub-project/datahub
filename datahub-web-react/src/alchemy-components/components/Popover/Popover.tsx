import * as React from 'react';

import FloatingOverlay, {
    FloatingOverlayProps,
    OverlayPlacement,
} from '@components/components/FloatingOverlay/FloatingOverlay';

export type PopoverPlacement = OverlayPlacement;

export type PopoverProps = Omit<FloatingOverlayProps, 'content'> & {
    content?: React.ReactNode | (() => React.ReactNode);
    title?: React.ReactNode | (() => React.ReactNode);
};

function resolveContent(content: PopoverProps['content']): React.ReactNode {
    return typeof content === 'function' ? content() : content;
}

export default function Popover({ title, content, showArrow = false, overlayInnerStyle, ...props }: PopoverProps) {
    const resolvedTitle = resolveContent(title);
    const resolvedContent = resolveContent(content);
    const popoverContent =
        resolvedTitle || resolvedContent ? (
            <>
                {resolvedTitle && (
                    <div style={{ fontWeight: 600, marginBottom: resolvedContent ? 8 : 0 }}>{resolvedTitle}</div>
                )}
                {resolvedContent}
            </>
        ) : undefined;

    return (
        <FloatingOverlay
            content={popoverContent}
            role="dialog"
            showArrow={showArrow}
            overlayInnerStyle={{ fontFamily: 'Mulish', ...overlayInnerStyle }}
            {...props}
        />
    );
}
