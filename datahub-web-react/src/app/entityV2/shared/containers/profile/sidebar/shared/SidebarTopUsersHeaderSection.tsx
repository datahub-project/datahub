import React from 'react';
import { useEntityData } from '../../../../../../entity/shared/EntityContext';
import TopUsersFacepile from './TopUsersFacepile';

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
