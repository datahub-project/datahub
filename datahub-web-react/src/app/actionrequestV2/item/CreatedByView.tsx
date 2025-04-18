import { Avatar } from '@components';
import React from 'react';
import { Link } from 'react-router-dom';

import { HoverEntityTooltip } from '@src/app/recommendations/renderer/component/HoverEntityTooltip';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { ActionRequest, Entity, EntityType } from '@src/types.generated';

interface Props {
    actionRequest: ActionRequest;
}

function CreatedByView({ actionRequest }: Props) {
    const entityRegistry = useEntityRegistryV2();

    const createdBy = actionRequest.created.actor;
    const createdByDisplayName =
        (createdBy && entityRegistry.getDisplayName(EntityType.CorpUser, createdBy)) || 'Anonymous';
    const createdByDisplayImage = createdBy && createdBy.editableInfo?.pictureLink;
    const createdByProfileUrl = `/${entityRegistry.getPathName(EntityType.CorpUser)}/${createdBy?.urn}`;

    return (
        <HoverEntityTooltip entity={createdBy as Entity} showArrow={false}>
            <Link to={createdByProfileUrl}>
                <Avatar name={createdByDisplayName} showInPill imageUrl={createdByDisplayImage} isOutlined />
            </Link>
        </HoverEntityTooltip>
    );
}

export default CreatedByView;
