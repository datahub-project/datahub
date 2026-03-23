import React from 'react';

import { ExploreLineageAction } from '@app/entityV2/shared/containers/profile/sidebar/ExploreLineageAction';
import { SidebarTitleActionType } from '@app/entityV2/shared/utils';

interface Props {
    actionType: SidebarTitleActionType;
}

export const TitleAction = ({ actionType }: Props) => {
    return <>{actionType === SidebarTitleActionType.LineageExplore && <ExploreLineageAction />}</>;
};
