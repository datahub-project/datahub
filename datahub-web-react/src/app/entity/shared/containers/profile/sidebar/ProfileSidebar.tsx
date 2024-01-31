import React, { useState } from 'react';
import styled from 'styled-components';
import { ProfileSidebarResizer } from './ProfileSidebarResizer';
import { EntitySidebar } from './EntitySidebar';
import { EntitySidebarSection } from '../../../types';

export const MAX_SIDEBAR_WIDTH = 800;
export const MIN_SIDEBAR_WIDTH = 200;

const Sidebar = styled.div<{ $width: number; backgroundColor?: string }>`
    max-height: 100%;
    position: relative;
    width: ${(props) => props.$width}px;
    min-width: ${(props) => props.$width}px;
    ${(props) => props.backgroundColor && `background-color: ${props.backgroundColor};`}
`;

const ScrollWrapper = styled.div`
    overflow: auto;
    max-height: 100%;
    padding: 0 20px 20px 20px;
`;

const DEFAULT_SIDEBAR_SECTION = {
    visible: (_, _1) => true,
};

interface Props {
    sidebarSections: EntitySidebarSection[];
    backgroundColor?: string;
    topSection?: EntitySidebarSection;
    alignLeft?: boolean;
}

export default function ProfileSidebar({ sidebarSections, backgroundColor, topSection, alignLeft }: Props) {
    const sideBarSectionsWithDefaults = sidebarSections.map((sidebarSection) => ({
        ...sidebarSection,
        display: { ...DEFAULT_SIDEBAR_SECTION, ...sidebarSection.display },
    }));

    const [sidebarWidth, setSidebarWidth] = useState(window.innerWidth * 0.25);

    if (alignLeft) {
        return (
            <>
                <Sidebar $width={sidebarWidth} backgroundColor={backgroundColor} id="entity-profile-sidebar">
                    <ScrollWrapper>
                        <EntitySidebar sidebarSections={sideBarSectionsWithDefaults} topSection={topSection} />
                    </ScrollWrapper>
                </Sidebar>
                <ProfileSidebarResizer
                    setSidePanelWidth={(width) =>
                        setSidebarWidth(Math.min(Math.max(width, MIN_SIDEBAR_WIDTH), MAX_SIDEBAR_WIDTH))
                    }
                    initialSize={sidebarWidth}
                    isSidebarOnLeft
                />
            </>
        );
    }

    return (
        <>
            <ProfileSidebarResizer
                setSidePanelWidth={(width) =>
                    setSidebarWidth(Math.min(Math.max(width, MIN_SIDEBAR_WIDTH), MAX_SIDEBAR_WIDTH))
                }
                initialSize={sidebarWidth}
            />
            <Sidebar $width={sidebarWidth} backgroundColor={backgroundColor} id="entity-profile-sidebar">
                <ScrollWrapper>
                    <EntitySidebar sidebarSections={sideBarSectionsWithDefaults} topSection={topSection} />
                </ScrollWrapper>
            </Sidebar>
        </>
    );
}
