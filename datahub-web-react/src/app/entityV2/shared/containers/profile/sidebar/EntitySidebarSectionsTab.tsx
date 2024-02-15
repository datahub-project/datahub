import React from 'react';
import { EntitySidebarSections } from './EntitySidebar';
import { EntitySidebarSection, TabContextType, TabRenderType } from '../../../types';

const DEFAULT_SIDEBAR_SECTION = {
    visible: (_, _1) => true,
};

interface Props {
    properties?: {
        sections: EntitySidebarSection[];
        setIsFormModalVisible: (visible: boolean) => void;
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
            <EntitySidebarSections
                renderType={renderType}
                contextType={contextType}
                sidebarSections={sectionsWithDefaults}
            />
        </>
    );
}
