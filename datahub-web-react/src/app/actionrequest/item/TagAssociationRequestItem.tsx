import React from 'react';
import { Link } from 'react-router-dom';
import { ActionRequest, EntityType } from '../../../types.generated';
import { StyledTag } from '../../entity/shared/components/styled/StyledTag';
import { useEntityRegistry } from '../../useEntityRegistry';
import AddContentView from './AddContentView';
import MetadataAssociationRequestItem from './MetadataAssociationRequestItem';

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

    const tag = actionRequest.params?.tagProposal?.tag;
    const tagView = tag && (
        <Link to={`/${entityRegistry.getPathName(EntityType.Tag)}/${tag.urn}`}>
            <StyledTag
                $color={tag?.properties?.colorHex}
                $colorHash={tag.urn}
                style={{ marginRight: 2, marginLeft: 2 }}
            >
                {tag?.name}
            </StyledTag>
        </Link>
    );

    const contentView = (
        <AddContentView
            showActionsButtons={showActionsButtons}
            requestMetadataView={tagView}
            actionRequest={actionRequest}
        />
    );

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
