import React from 'react';
import styled from 'styled-components';

import { EntitySidebarSections } from '@app/entityV2/shared/containers/profile/sidebar/EntitySidebar';
import SidebarFormInfoWrapper from '@app/entityV2/shared/containers/profile/sidebar/FormInfo/SidebarFormInfoWrapper';
import { EntitySidebarSection, TabContextType, TabRenderType } from '@app/entityV2/shared/types';

const DEFAULT_SIDEBAR_SECTION = {
    visible: (_, _1) => true,
};

const SidebarFormContentWrapper = styled.div`
    padding: 0px 20px;
`;

interface Props {
    properties?: {
        sections: EntitySidebarSection[];
        setIsFormModalVisible?: (visible: boolean) => void;
    };
    contextType: TabContextType;
    renderType: TabRenderType;
}

export const contextsWithoutLastSynchronized = [
    TabContextType.CHROME_SIDEBAR,
    TabContextType.SEARCH_SIDEBAR,
    TabContextType.LINEAGE_SIDEBAR,
    TabContextType.FORM_SIDEBAR,
];

export default function EntitySidebarSectionsTab({ properties, contextType, renderType }: Props) {
    const sections = properties?.sections || [];

    const sectionsWithDefaults = sections.map((sidebarSection) => ({
        ...sidebarSection,
        display: { ...DEFAULT_SIDEBAR_SECTION, ...sidebarSection.display },
    }));

    return (
        <>
            <SidebarFormContentWrapper>
                <SidebarFormInfoWrapper />
            </SidebarFormContentWrapper>
            <EntitySidebarSections
                renderType={renderType}
                contextType={contextType}
                sidebarSections={sectionsWithDefaults}
            />
        </>
    );
}
