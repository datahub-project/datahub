import React from 'react';
import { useEntityData } from '../../../../../EntityContext';
import { SidebarHeaderSectionColumns } from '../../SidebarHeaderSectionColumns';
import SidebarPopularityHeaderSection from './SidebarPopularityHeaderSection';
import SidebarTopUsersHeaderSection from '../../shared/SidebarTopUsersHeaderSection';
import { isValuePresent, userExists } from '../../shared/utils';
import CompactContext from '../../../../../../../shared/CompactContext';

const SidebarDatasetHeaderSection = () => {
    const { entityData } = useEntityData();
    const dataset = entityData as any;

    const isCompact = React.useContext(CompactContext);

    const columns: any = [];

    /**
     * Popularity tab
     */
    if (
        isValuePresent(dataset?.statsSummary?.queryCountPercentileLast30Days) &&
        isValuePresent(dataset?.statsSummary?.uniqueUserPercentileLast30Days)
    ) {
        columns.push({
            title: 'Popularity',
            content: <SidebarPopularityHeaderSection />,
        });
    }

    /**
     * Top users tab
     */
    if (
        dataset?.statsSummary?.topUsersLast30Days &&
        dataset?.statsSummary?.topUsersLast30Days?.find((user) => userExists(user))
    ) {
        columns.push({
            title: 'Top Users',
            content: <SidebarTopUsersHeaderSection />,
        });
    }

    if (!columns.length && !isCompact) {
        return null;
    }

    return <SidebarHeaderSectionColumns columns={columns} />;
};

export default SidebarDatasetHeaderSection;
