/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useCallback, useState } from 'react';
import styled from 'styled-components';

let isResizing = false;

export type Props = {
    children: React.ReactNode;
};

const ResizableDiv = styled.div<{ width }>`
    width: ${(props) => props.width}px;
    min-width: 440px;
    display: flex;
    justify-content: space-between;
`;

const HeaderAndTabs = ({ children }: Props) => {
    const initialWidth = 70 / (100 / document.documentElement.clientWidth);

    const [sidebarWidth, setSidebarWidth] = useState(initialWidth);

    const cbHandleMouseMove = useCallback((e) => {
        const offsetRight = e.clientX - document.body.offsetLeft;

        const minWidthVw = 70;
        const minWidthPx = minWidthVw / (100 / document.documentElement.clientWidth);

        const maxWidthVw = 98;
        const maxWidthPx = maxWidthVw / (100 / document.documentElement.clientWidth);

        if (offsetRight > minWidthPx && offsetRight < maxWidthPx) {
            setSidebarWidth(offsetRight);
        }
    }, []);

    const cbHandleMouseUp = useCallback(
        (_) => {
            if (!isResizing) {
                return;
            }
            isResizing = false;
            document.removeEventListener('mousemove', cbHandleMouseMove);
            document.removeEventListener('mouseup', cbHandleMouseUp);
        },
        [cbHandleMouseMove],
    );

    function handleMousedown(e) {
        e.stopPropagation();
        e.preventDefault();
        // we will only add listeners when needed, and remove them afterward
        document.addEventListener('mousemove', cbHandleMouseMove);
        document.addEventListener('mouseup', cbHandleMouseUp);
        isResizing = true;
    }

    return (
        <ResizableDiv width={sidebarWidth}>
            {children}
            {/* eslint-disable jsx-a11y/no-static-element-interactions */}
            <div onMouseDown={handleMousedown} style={{ backgroundColor: 'black', width: '5px', height: '100px' }}>
                header
            </div>
        </ResizableDiv>
    );
};

export default HeaderAndTabs;
