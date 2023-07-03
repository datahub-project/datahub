import React, { useState } from 'react';
import styled from 'styled-components/macro';
import GlossarySearch from './GlossarySearch';
import GlossaryBrowser from './GlossaryBrowser/GlossaryBrowser';
import { ProfileSidebarResizer } from '../entity/shared/containers/profile/sidebar/ProfileSidebarResizer';

const BrowserWrapper = styled.div<{ width: number }>`
    max-height: 100%;
    width: ${(props) => props.width}px;
    min-width: ${(props) => props.width}px;
`;

export const MAX_BROWSER_WIDTH = 500;
export const MIN_BROWSWER_WIDTH = 200;

export default function GlossarySidebar() {
    const [browserWidth, setBrowserWith] = useState(window.innerWidth * 0.2);

    return (
        <>
            <BrowserWrapper width={browserWidth}>
                <GlossarySearch />
                <GlossaryBrowser openToEntity />
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
