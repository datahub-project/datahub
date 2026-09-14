import React, { useCallback, useEffect, useRef, useState } from 'react';
import styled from 'styled-components';

/** Attached to a column by resolveSchemaTableColumns via antd `onHeaderCell`. */
export interface HeaderResizeProps {
    columnId: string;
    minWidth: number;
    onResizeEnd: (columnId: string, width: number) => void;
    onReset: (columnId: string) => void;
    hint?: string;
}

const Grip = styled.span<{ $active: boolean }>`
    position: absolute;
    top: 0;
    right: -3px;
    width: 6px;
    height: 100%;
    cursor: col-resize;
    user-select: none;
    z-index: 2;
    background: ${(props) => (props.$active ? props.theme.colors.borderHover : 'transparent')};

    &:hover {
        background: ${(props) => props.theme.colors.borderHover};
    }
`;

const Guide = styled.div`
    position: fixed;
    top: 0;
    bottom: 0;
    width: 1px;
    pointer-events: none;
    z-index: 1000;
    background: ${(props) => props.theme.colors.borderHover};
`;

type DragState = { startX: number; startWidth: number; cellLeft: number; latest: number };

type Props = React.ThHTMLAttributes<HTMLTableCellElement> & { colviewResize?: HeaderResizeProps };

/**
 * antd header cell with a drag grip on its right edge. A guide line previews the new edge while
 * dragging; the width is committed on mouseup so the table isn't re-laid-out per mousemove.
 * Double-click clears the saved width. Touch users set widths from the column gear instead.
 */
export default function ResizableHeaderCell({ colviewResize, children, style, ...rest }: Props) {
    const cellRef = useRef<HTMLTableCellElement>(null);
    const drag = useRef<DragState | null>(null);
    // Set for the duration of a drag: removes the window listeners registered on mousedown and
    // restores text selection. Run on mouseup, or on unmount if the cell goes away mid-drag.
    const release = useRef<(() => void) | null>(null);
    const [guideX, setGuideX] = useState<number | null>(null);

    useEffect(() => () => release.current?.(), []);

    const onMove = useCallback(
        (event: MouseEvent) => {
            if (!drag.current || !colviewResize) return;
            const { startX, startWidth, cellLeft } = drag.current;
            const next = Math.max(colviewResize.minWidth, startWidth + (event.clientX - startX));
            drag.current.latest = next;
            setGuideX(cellLeft + next);
        },
        [colviewResize],
    );

    const onUp = useCallback(() => {
        release.current?.();
        setGuideX(null);
        if (drag.current && colviewResize && drag.current.latest !== drag.current.startWidth) {
            colviewResize.onResizeEnd(colviewResize.columnId, Math.round(drag.current.latest));
        }
        drag.current = null;
    }, [colviewResize]);

    const onMouseDown = useCallback(
        (event: React.MouseEvent) => {
            const rect = cellRef.current?.getBoundingClientRect();
            if (!rect || !colviewResize) return;
            drag.current = { startX: event.clientX, startWidth: rect.width, cellLeft: rect.left, latest: rect.width };
            document.body.style.userSelect = 'none';
            window.addEventListener('mousemove', onMove);
            window.addEventListener('mouseup', onUp);
            release.current = () => {
                window.removeEventListener('mousemove', onMove);
                window.removeEventListener('mouseup', onUp);
                document.body.style.userSelect = '';
                release.current = null;
            };
            // Don't let the press reach antd's header, which would toggle the column sorter.
            event.preventDefault();
            event.stopPropagation();
        },
        [colviewResize, onMove, onUp],
    );

    if (!colviewResize) {
        return (
            <th {...rest} style={style}>
                {children}
            </th>
        );
    }

    return (
        <th {...rest} ref={cellRef} style={{ ...style, position: 'relative' }}>
            {children}
            <Grip
                $active={guideX !== null}
                title={colviewResize.hint}
                onMouseDown={onMouseDown}
                onClick={(event) => event.stopPropagation()}
                onDoubleClick={(event) => {
                    event.stopPropagation();
                    colviewResize.onReset(colviewResize.columnId);
                }}
                data-testid={`colview-resize-${colviewResize.columnId}`}
            />
            {guideX !== null && <Guide style={{ left: guideX }} />}
        </th>
    );
}
