import React, { useEffect, useState } from 'react';
import GlossarySearch from './GlossarySearch';
import GlossaryBrowser from './GlossaryBrowser/GlossaryBrowser';
import { SidebarWrapper } from '../sharedV2/sidebar/components';
import { useGlossaryEntityData } from '../entityV2/shared/GlossaryEntityContext';

export default function GlossarySidebar() {
    const [browserWidth, setBrowserWidth] = useState(window.innerWidth * 0.2);
    const [previousBrowserWidth] = useState(window.innerWidth * 0.2);
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
        </>
    );
}
