import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import React from 'react';
import styled from 'styled-components';

type Props = {
    setSidePanelWidth: (width: number) => void;
    initialSize: number;
    isSidebarOnLeft?: boolean;
};

const ResizerBar = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    min-height: 100%;
    width: 4px;
    cursor: col-resize;
    ${(props) => !props.$isShowNavBarRedesign && 'margin-right: 12px;'}
`;

export const ProfileSidebarResizer = ({ setSidePanelWidth, initialSize, isSidebarOnLeft }: Props) => {
    const isShowNavBarRedesign = useShowNavBarRedesign();
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
            $isShowNavBarRedesign={isShowNavBarRedesign}
        />
    );
};
