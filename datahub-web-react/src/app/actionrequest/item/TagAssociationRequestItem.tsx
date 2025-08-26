import React from 'react';
import { Link } from 'react-router-dom';

import AddContentView from '@app/actionrequest/item/AddContentView';
import MetadataAssociationRequestItem from '@app/actionrequest/item/MetadataAssociationRequestItem';
import { StyledTag } from '@app/entity/shared/components/styled/StyledTag';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { ActionRequest, EntityType } from '@types';

type Props = {
    actionRequest: ActionRequest;
    onUpdate: () => void;
    showActionsButtons: boolean;
};

const REQUEST_TYPE_DISPLAY_NAME = 'Tag Proposal';

/**
 * A list item representing a tag proposal request.
 */
export default function TagAssociationRequestItem({ actionRequest, onUpdate, showActionsButtons }: Props) {
    const entityRegistry = useEntityRegistry();

    const tag = actionRequest.params?.tagProposal?.tag || actionRequest.params?.tagProposal?.tags?.[0];
    const tagView = tag && (
        <Link to={`/${entityRegistry.getPathName(EntityType.Tag)}/${tag.urn}`}>
            <StyledTag
                $color={tag?.properties?.colorHex}
                $colorHash={tag.urn}
                style={{ marginRight: 2, marginLeft: 2 }}
            >
                {entityRegistry.getDisplayName(EntityType.Tag, tag)}
            </StyledTag>
        </Link>
    );

    const contentView = <AddContentView requestMetadataViews={[{ primary: tagView }]} actionRequest={actionRequest} />;

    return (
        <MetadataAssociationRequestItem
            requestTypeDisplayName={REQUEST_TYPE_DISPLAY_NAME}
            requestContentView={contentView}
            actionRequest={actionRequest}
            onUpdate={onUpdate}
            showActionsButtons={showActionsButtons}
        />
    );
}
