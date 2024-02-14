import React from 'react';
import styled from 'styled-components';
import { Divider } from 'antd';
import { EntitySidebarSections } from './EntitySidebar';
import { EntitySidebarSection, TabContextType, TabRenderType } from '../../../types';
import { EntityHeader } from '../header/EntityHeader';
import { EntityMenuItems } from '../../../EntityDropdown/EntityMenuActions';

const Header = styled.div``;

const HeaderDivider = styled(Divider)`
    margin-bottom: 0px;
    margin-top: 8px;
`;

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

const contextsWithoutLastSynchronized = [
    TabContextType.CHROME_SIDEBAR,
    TabContextType.SEARCH_SIDEBAR,
    TabContextType.LINEAGE_SIDEBAR,
    TabContextType.FORM_SIDEBAR,
];

const contextsWithoutHeader = [TabContextType.PROFILE, TabContextType.PROFILE_SIDEBAR];

export default function EntitySidebarSectionsTab({ properties, contextType, renderType }: Props) {
    const sections = properties?.sections || [];

    const sectionsWithDefaults = sections.map((sidebarSection) => ({
        ...sidebarSection,
        display: { ...DEFAULT_SIDEBAR_SECTION, ...sidebarSection.display },
    }));

    const showEntityHeader = !contextsWithoutHeader.includes(contextType);

    return (
        <>
            {showEntityHeader && (
                <Header>
                    <EntityHeader isCompact headerDropdownItems={new Set([EntityMenuItems.EXTERNAL_URL])} />
                    <HeaderDivider />
                </Header>
            )}
            <EntitySidebarSections
                renderType={renderType}
                contextType={contextType}
                sidebarSections={sectionsWithDefaults}
                hideLastSynchronized={contextsWithoutLastSynchronized.includes(contextType) || false}
            />
        </>
    );
}
