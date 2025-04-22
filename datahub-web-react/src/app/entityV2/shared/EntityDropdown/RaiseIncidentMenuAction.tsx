import { WarningOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import React, { useState } from 'react';
import { useHistory } from 'react-router';

import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import { ActionMenuItem } from '@app/entityV2/shared/EntityDropdown/styledComponents';
import { getEntityPath } from '@app/entityV2/shared/containers/profile/utils';
import { AddIncidentModal } from '@app/entityV2/shared/tabs/Incident/components/AddIncidentModal';
import { useIsSeparateSiblingsMode } from '@app/entityV2/shared/useIsSeparateSiblingsMode';
import { useEntityRegistry } from '@app/useEntityRegistry';

export default function RaiseIncidentMenuAction() {
    const { urn, entityType } = useEntityData();
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
                <AddIncidentModal
                    urn={urn}
                    entityType={entityType}
                    visible={isRaiseIncidentModalVisible}
                    onClose={() => setIsRaiseIncidentModalVisible(false)}
                    refetch={
                        (() => {
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
                        }) as any
                    }
                />
            )}
        </Tooltip>
    );
}
