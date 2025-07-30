import React from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import TopUsersFacepile from '@app/entityV2/shared/containers/profile/sidebar/shared/TopUsersFacepile';

const SidebarTopUsersHeaderSection = () => {
    const { entityData } = useEntityData();
    const dataset = entityData as any;
    const statsSummary = dataset?.statsSummary;
    const topUsersLast30Days = statsSummary?.topUsersLast30Days;

    if (!topUsersLast30Days) {
        return null;
    }

    return <TopUsersFacepile users={topUsersLast30Days} max={3} />;
};

export default SidebarTopUsersHeaderSection;
