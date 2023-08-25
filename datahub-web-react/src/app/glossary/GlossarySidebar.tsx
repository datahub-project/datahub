import React, { useState } from 'react';
import GlossarySearch from './GlossarySearch';
import GlossaryBrowser from './GlossaryBrowser/GlossaryBrowser';
import { ProfileSidebarResizer } from '../entity/shared/containers/profile/sidebar/ProfileSidebarResizer';
import { SidebarWrapper } from '../shared/sidebar/components';

export const MAX_BROWSER_WIDTH = 500;
export const MIN_BROWSWER_WIDTH = 200;

export default function GlossarySidebar() {
    const [browserWidth, setBrowserWith] = useState(window.innerWidth * 0.2);

    return (
        <>
            <SidebarWrapper width={browserWidth}>
                <GlossarySearch />
                <GlossaryBrowser openToEntity />
            </SidebarWrapper>
            <ProfileSidebarResizer
                setSidePanelWidth={(width) =>
                    setBrowserWith(Math.min(Math.max(width, MIN_BROWSWER_WIDTH), MAX_BROWSER_WIDTH))
                }
                initialSize={browserWidth}
                isSidebarOnLeft
            />
        </>
    );
}
