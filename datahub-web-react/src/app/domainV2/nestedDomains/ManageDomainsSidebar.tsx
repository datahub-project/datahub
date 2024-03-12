import React, { useState, useEffect } from 'react';
import DomainsSidebarHeader from './DomainsSidebarHeader';
import { SidebarWrapper } from '../../sharedV2/sidebar/components';
import DomainNavigator from './domainNavigator/DomainNavigator';
import DomainSearch from '../DomainSearch';

export default function ManageDomainsSidebarV2() {
    const [browserWidth, setBrowserWidth] = useState(window.innerWidth * 0.2);

    useEffect(() => {
        const handleResize = () => {
            setBrowserWidth(window.innerWidth * 0.2);
        };

        window.addEventListener('resize', handleResize);

        return () => {
            window.removeEventListener('resize', handleResize);
        };
    }, []);

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
