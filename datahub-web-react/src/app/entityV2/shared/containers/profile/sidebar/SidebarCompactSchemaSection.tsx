import React from 'react';
import styled from 'styled-components';
import { ENTITY_PROFILE_TAGS_ID } from '../../../../../onboarding/config/EntityProfileOnboardingConfig';
import { SidebarSection } from './SidebarSection';
import { TabRenderType } from '../../../types';
import { SchemaTab } from '../../../tabs/Dataset/Schema/SchemaTab';

const Content = styled.div`
    display: flex;
    align-items: start;
    justify-content: start;
    flex-wrap: wrap;
`;

export const SidebarCompactSchemaSection = () => {
    return (
        <div id={ENTITY_PROFILE_TAGS_ID}>
            <SidebarSection
                title="Fields"
                content={
                    <Content>
                        <SchemaTab renderType={TabRenderType.COMPACT} />
                    </Content>
                }
            />
        </div>
    );
};
