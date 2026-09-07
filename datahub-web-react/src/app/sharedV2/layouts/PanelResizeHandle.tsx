import React, { useCallback, useRef } from 'react';
import styled from 'styled-components';

const HandleBar = styled.div`
    min-height: 100%;
    width: 4px;
    flex-shrink: 0;
    cursor: col-resize;
    background: transparent;
    /* Pull tight against the panel it resizes, rather than floating in the middle
       of the flex container's gap. */
    margin-right: -4px;

    &:hover {
        background: ${(props) => props.theme.colors.borderHover};
    }
`;

type Props = {
    getInitialWidth: () => number;
    onResize: (width: number) => void;
    onResizeEnd?: (width: number) => void;
    isSidebarOnLeft?: boolean;
};

export function PanelResizeHandle({ getInitialWidth, onResize, onResizeEnd, isSidebarOnLeft }: Props) {
    const dragState = useRef<{ initialX: number; initialWidth: number; latestWidth: number } | null>(null);

    const dragContinue = useCallback(
        (event: MouseEvent) => {
            if (!dragState.current) return;
            const { initialX, initialWidth } = dragState.current;
            // For a right-docked panel (the default), the handle sits on the panel's left edge:
            // dragging left grows the panel, dragging right shrinks it.
            const xDifference = isSidebarOnLeft ? initialX - event.clientX : event.clientX - initialX;
            const nextWidth = initialWidth - xDifference;
            dragState.current.latestWidth = nextWidth;
            onResize(nextWidth);
        },
        [isSidebarOnLeft, onResize],
    );

    const stopDragging = useCallback(() => {
        window.removeEventListener('mousemove', dragContinue);
        window.removeEventListener('mouseup', stopDragging);
        document.body.style.userSelect = '';
        if (dragState.current) {
            onResizeEnd?.(dragState.current.latestWidth);
        }
        dragState.current = null;
    }, [dragContinue, onResizeEnd]);

    const onMouseDown = useCallback(
        (event: React.MouseEvent) => {
            dragState.current = {
                initialX: event.clientX,
                initialWidth: getInitialWidth(),
                latestWidth: getInitialWidth(),
            };
            document.body.style.userSelect = 'none';
            window.addEventListener('mousemove', dragContinue);
            window.addEventListener('mouseup', stopDragging);
            event.preventDefault();
        },
        [getInitialWidth, dragContinue, stopDragging],
    );

    return <HandleBar onMouseDown={onMouseDown} data-testid="panel-resize-handle" />;
}
