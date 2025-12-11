/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useEffect, useState } from 'react';

import { ProfileSidebarResizer } from '@app/entity/shared/containers/profile/sidebar/ProfileSidebarResizer';
import { useGlossaryEntityData } from '@app/entityV2/shared/GlossaryEntityContext';
import GlossaryBrowser from '@app/glossary/GlossaryBrowser/GlossaryBrowser';
import GlossarySearch from '@app/glossary/GlossarySearch';
import { SidebarWrapper } from '@app/shared/sidebar/components';

export const MAX_BROWSER_WIDTH = 500;
export const MIN_BROWSWER_WIDTH = 200;

export default function GlossarySidebar() {
    const [browserWidth, setBrowserWidth] = useState(window.innerWidth * 0.2);
    const [previousBrowserWidth, setPreviousBrowserWidth] = useState(window.innerWidth * 0.2);
    const { isSidebarOpen } = useGlossaryEntityData();

    useEffect(() => {
        if (isSidebarOpen) {
            setBrowserWidth(previousBrowserWidth);
        } else {
            setBrowserWidth(0);
        }
    }, [isSidebarOpen, previousBrowserWidth]);

    return (
        <>
            <SidebarWrapper width={browserWidth} data-testid="glossary-browser-sidebar">
                <GlossarySearch />
                <GlossaryBrowser openToEntity />
            </SidebarWrapper>
            <ProfileSidebarResizer
                setSidePanelWidth={(width) => {
                    const newWidth = Math.min(Math.max(width, MIN_BROWSWER_WIDTH), MAX_BROWSER_WIDTH);
                    setBrowserWidth(newWidth);
                    setPreviousBrowserWidth(newWidth);
                }}
                initialSize={browserWidth}
                isSidebarOnLeft
            />
        </>
    );
}
