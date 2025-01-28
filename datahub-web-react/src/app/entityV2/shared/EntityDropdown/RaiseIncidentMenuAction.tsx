import React, { useState } from 'react';
import { WarningOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import { useHistory } from 'react-router';
import { useEntityData, useRefetch } from '../../../entity/shared/EntityContext';
import { ActionMenuItem } from './styledComponents';
import { AddIncidentModal } from '../tabs/Incident/components/AddIncidentModal';
import { getEntityPath } from '../containers/profile/utils';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { useIsSeparateSiblingsMode } from '../useIsSeparateSiblingsMode';

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
