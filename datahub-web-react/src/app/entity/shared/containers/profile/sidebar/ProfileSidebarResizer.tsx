import React from 'react';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../../constants';

type Props = {
    setSidePanelWidth: (width: number) => void;
    initialSize: number;
    isSidebarOnLeft?: boolean;
};

const ResizerBar = styled.div`
    min-height: 100%;
    border: 1px solid ${ANTD_GRAY[4]};
    cursor: col-resize;
`;
export const ProfileSidebarResizer = ({ setSidePanelWidth, initialSize, isSidebarOnLeft }: Props) => {
    let dragState: { initialX: number; initialSize: number } | undefined;

    const dragContinue = (event: MouseEvent) => {
        if (!dragState) {
            return;
        }

        let xDifference = event.clientX - (dragState.initialX || 0);
        if (isSidebarOnLeft) {
            xDifference = (dragState.initialX || 0) - event.clientX;
        }
        setSidePanelWidth(dragState.initialSize - xDifference);
    };

    const stopDragging = () => {
        window.removeEventListener('mousemove', dragContinue, false);
        window.removeEventListener('mouseup', stopDragging, false);
    };

    const onDrag = (event: React.MouseEvent) => {
        const { clientX } = event;
        dragState = { initialX: clientX, initialSize };

        window.addEventListener('mousemove', dragContinue, false);
        window.addEventListener('mouseup', stopDragging, false);
        event.preventDefault();
    };

    return (
        <ResizerBar
            onMouseDown={(event) => {
                onDrag(event);
            }}
        />
    );
};
