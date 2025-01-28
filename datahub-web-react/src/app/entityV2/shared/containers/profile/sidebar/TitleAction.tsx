import React from 'react';
import { SidebarTitleActionType } from '../../../utils';
import { ExploreLineageAction } from './ExploreLineageAction';

interface Props {
    actionType: SidebarTitleActionType;
    icon?: React.FC<any>;
}

export const TitleAction = ({ actionType, icon }: Props) => {
    return <>{actionType === SidebarTitleActionType.LineageExplore && <ExploreLineageAction icon={icon} />}</>;
};
