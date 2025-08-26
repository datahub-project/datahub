import { WarningOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import React, { useState } from 'react';
import { useHistory } from 'react-router';

import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import { ActionMenuItem } from '@app/entityV2/shared/EntityDropdown/styledComponents';
import { getEntityPath } from '@app/entityV2/shared/containers/profile/utils';
import { IncidentDetailDrawer } from '@app/entityV2/shared/tabs/Incident/AcrylComponents/IncidentDetailDrawer';
import { IncidentAction } from '@app/entityV2/shared/tabs/Incident/constant';
import { useIsSeparateSiblingsMode } from '@app/entityV2/shared/useIsSeparateSiblingsMode';
import { useEntityRegistry } from '@app/useEntityRegistry';

export default function RaiseIncidentMenuAction() {
    const { urn, entityType, entityData } = useEntityData();
    const refetchForEntity = useRefetch();
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const isHideSiblingMode = useIsSeparateSiblingsMode();
    const [isRaiseIncidentModalVisible, setIsRaiseIncidentModalVisible] = useState(false);

    return (
        <Tooltip placement="bottom" title="Raise an incident">
            <ActionMenuItem key="incident" disabled={false} onClick={() => setIsRaiseIncidentModalVisible(true)}>
                <WarningOutlined style={{ display: 'flex' }} />
            </ActionMenuItem>
            {isRaiseIncidentModalVisible && (
                <IncidentDetailDrawer
                    entity={{ urn, entityType, platform: entityData?.platform ?? undefined }}
                    mode={IncidentAction.CREATE}
                    onCancel={() => setIsRaiseIncidentModalVisible(false)}
                    onSubmit={() => {
                        setIsRaiseIncidentModalVisible(false);
                        setTimeout(() => {
                            refetchForEntity?.();
                            history.push(
                                `${getEntityPath(
                                    entityType,
                                    urn,
                                    entityRegistry,
                                    false,
                                    isHideSiblingMode,
                                    'Incidents',
                                )}`,
                            );
                        }, 3000);
                    }}
                />
            )}
        </Tooltip>
    );
}
