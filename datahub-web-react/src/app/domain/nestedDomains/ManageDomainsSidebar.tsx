import React, { useState } from 'react';
import { BrowserWrapper, MAX_BROWSER_WIDTH, MIN_BROWSWER_WIDTH } from '../../glossary/BusinessGlossaryPage';
import { ProfileSidebarResizer } from '../../entity/shared/containers/profile/sidebar/ProfileSidebarResizer';
import DomainsSidebarHeader from './DomainsSidebarHeader';

export default function ManageDomainsSidebar() {
    const [browserWidth, setBrowserWith] = useState(window.innerWidth * 0.2);

    return (
        <>
            <BrowserWrapper width={browserWidth}>
                <DomainsSidebarHeader />
            </BrowserWrapper>
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
