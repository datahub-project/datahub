import React, { useState } from 'react';
import { MAX_BROWSER_WIDTH, MIN_BROWSWER_WIDTH } from '../../glossary/BusinessGlossaryPage';
import { ProfileSidebarResizer } from '../../entity/shared/containers/profile/sidebar/ProfileSidebarResizer';
import DomainsSidebarHeader from './DomainsSidebarHeader';
import { SidebarWrapper } from '../../shared/sidebar/components';
import DomainNavigator from './domainNavigator/DomainNavigator';
import DomainSearch from '../DomainSearch';

export default function ManageDomainsSidebar() {
    const [browserWidth, setBrowserWith] = useState(window.innerWidth * 0.2);

    return (
        <>
            <SidebarWrapper width={browserWidth}>
                <DomainsSidebarHeader />
                <DomainSearch />
                <DomainNavigator />
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
