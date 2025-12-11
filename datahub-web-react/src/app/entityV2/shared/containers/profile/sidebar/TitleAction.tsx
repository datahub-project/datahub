/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { ExploreLineageAction } from '@app/entityV2/shared/containers/profile/sidebar/ExploreLineageAction';
import { SidebarTitleActionType } from '@app/entityV2/shared/utils';

interface Props {
    actionType: SidebarTitleActionType;
    icon?: React.FC<any>;
}

export const TitleAction = ({ actionType, icon }: Props) => {
    return <>{actionType === SidebarTitleActionType.LineageExplore && <ExploreLineageAction icon={icon} />}</>;
};
