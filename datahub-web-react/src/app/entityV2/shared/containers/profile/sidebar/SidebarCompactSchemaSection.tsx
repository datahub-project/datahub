import React from 'react';
import { ENTITY_PROFILE_SCHEMA_ID } from '../../../../../onboarding/config/EntityProfileOnboardingConfig';
import { SidebarSection } from './SidebarSection';
import { TabRenderType } from '../../../types';
import { SchemaTab } from '../../../tabs/Dataset/Schema/SchemaTab';

export const SidebarCompactSchemaSection = () => {
    return (
        <div id={ENTITY_PROFILE_SCHEMA_ID}>
            <SidebarSection title="Fields" content={<SchemaTab renderType={TabRenderType.COMPACT} />} />
        </div>
    );
};
