import { Button, Tooltip } from '@components';
import React, { useContext } from 'react';
import { useHistory } from 'react-router-dom';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { getEntityPath } from '@app/entityV2/shared/containers/profile/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';
import CompactContext from '@src/app/shared/CompactContext';

export const ExploreLineageAction = () => {
    const entityRegistry = useEntityRegistry();
    const history = useHistory();
    const isCompact = useContext(CompactContext);
    const { urn, entityType, entityData } = useEntityData();
    const entityName = (entityData && entityRegistry.getDisplayName(entityType, entityData)) || '-';
    const lineagePath = getEntityPath(entityType, urn, entityRegistry, false, false, 'Lineage');

    const handleClick = () => {
        if (isCompact) {
            window.open(lineagePath, '_blank');
        } else {
            history.push(lineagePath);
        }
    };

    return (
        <Tooltip
            placement="left"
            showArrow={false}
            title={`Visually explore the upstreams and downstreams of ${entityName}`}
        >
            <Button
                variant="text"
                color="violet"
                size="md"
                icon={{ icon: 'TreeStructure', source: 'phosphor' }}
                onClick={handleClick}
            />
        </Tooltip>
    );
};
