import React, { useCallback, useRef } from 'react';
import styled from 'styled-components';

type Props = {
    width: number;
    onWidthChange: (width: number) => void;
    onResizeStart?: () => void;
    onResizeEnd?: () => void;
};

/**
 * Absolute hit target on the sidebar’s right edge — no layout width.
 * Sits on the border (inside) so the page gap is unchanged.
 */
const ResizerBar = styled.div`
    position: absolute;
    top: 0;
    right: 0;
    bottom: 0;
    width: 8px;
    cursor: col-resize;
    z-index: 2;
`;

/** Left-sidebar drag handle — drag right to widen. */
export default function SidebarResizer({ width, onWidthChange, onResizeStart, onResizeEnd }: Props) {
    const dragRef = useRef<{ initialX: number; initialWidth: number } | null>(null);

    const onMouseDown = useCallback(
        (event: React.MouseEvent) => {
            dragRef.current = { initialX: event.clientX, initialWidth: width };
            onResizeStart?.();

            const onMove = (moveEvent: MouseEvent) => {
                const drag = dragRef.current;
                if (!drag) return;
                const delta = moveEvent.clientX - drag.initialX;
                onWidthChange(drag.initialWidth + delta);
            };

            const onUp = () => {
                dragRef.current = null;
                window.removeEventListener('mousemove', onMove);
                window.removeEventListener('mouseup', onUp);
                onResizeEnd?.();
            };

            window.addEventListener('mousemove', onMove);
            window.addEventListener('mouseup', onUp);
            event.preventDefault();
        },
        [width, onWidthChange, onResizeStart, onResizeEnd],
    );

    return <ResizerBar role="separator" aria-orientation="vertical" onMouseDown={onMouseDown} />;
}
