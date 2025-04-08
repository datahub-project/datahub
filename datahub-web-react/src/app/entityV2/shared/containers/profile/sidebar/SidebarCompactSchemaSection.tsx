import React from 'react';

import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { SchemaTab } from '@app/entityV2/shared/tabs/Dataset/Schema/SchemaTab';
import { TabRenderType } from '@app/entityV2/shared/types';
import { ENTITY_PROFILE_SCHEMA_ID } from '@app/onboarding/config/EntityProfileOnboardingConfig';

export const SidebarCompactSchemaSection = () => {
    return (
        <div id={ENTITY_PROFILE_SCHEMA_ID}>
            <SidebarSection title="Fields" content={<SchemaTab renderType={TabRenderType.COMPACT} />} />
        </div>
    );
};
