import React, { useState } from 'react';

import DomainSearch from '@app/domain/DomainSearch';
import DomainsSidebarHeader from '@app/domain/nestedDomains/DomainsSidebarHeader';
import DomainNavigator from '@app/domain/nestedDomains/domainNavigator/DomainNavigator';
import { ProfileSidebarResizer } from '@app/entity/shared/containers/profile/sidebar/ProfileSidebarResizer';
import { MAX_BROWSER_WIDTH, MIN_BROWSWER_WIDTH } from '@app/glossary/BusinessGlossaryPage';
import { SidebarWrapper } from '@app/shared/sidebar/components';

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
