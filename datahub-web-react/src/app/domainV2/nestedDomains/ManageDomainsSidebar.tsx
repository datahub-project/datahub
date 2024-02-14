import React, { useState } from 'react';
import DomainsSidebarHeader from './DomainsSidebarHeader';
import { SidebarWrapper } from '../../sharedV2/sidebar/components';
import DomainNavigator from './domainNavigator/DomainNavigator';
import DomainSearch from '../DomainSearch';

export default function ManageDomainsSidebarV2() {
    const [browserWidth] = useState(window.innerWidth * 0.2);

    return (
        <>
            <SidebarWrapper width={browserWidth}>
                <DomainsSidebarHeader />
                <DomainSearch />
                <DomainNavigator />
            </SidebarWrapper>
        </>
    );
}
