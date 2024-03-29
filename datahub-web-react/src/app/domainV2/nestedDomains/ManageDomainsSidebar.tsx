import React from 'react';
import useSidebarWidth from '../../sharedV2/sidebar/useSidebarWidth';
import DomainsSidebarHeader from './DomainsSidebarHeader';
import { SidebarWrapper } from '../../sharedV2/sidebar/components';
import DomainNavigator from './domainNavigator/DomainNavigator';
import DomainSearch from '../DomainSearch';

export default function ManageDomainsSidebarV2() {
    const width = useSidebarWidth(0.2);
    return (
        <>
            <SidebarWrapper width={width}>
                <DomainsSidebarHeader />
                <DomainSearch />
                <DomainNavigator />
            </SidebarWrapper>
        </>
    );
}
