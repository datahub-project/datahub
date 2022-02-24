import React from 'react';
import { Link } from 'react-router-dom';
import { ActionRequest, EntityType } from '../../../types.generated';
import { StyledTag } from '../../entity/shared/components/styled/StyledTag';
import { useEntityRegistry } from '../../useEntityRegistry';
import MetadataAssociationRequestItem from './MetadataAssociationRequestItem';

type Props = {
    actionRequest: ActionRequest;
    onUpdate: () => void;
};

const REQUEST_TYPE_DISPLAY_NAME = 'Tag Proposal';

/**
 * A list item representing a tag proposal request.
 */
export default function TagAssociationRequestItem({ actionRequest, onUpdate }: Props) {
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

    return (
        <MetadataAssociationRequestItem
            requestTypeDisplayName={REQUEST_TYPE_DISPLAY_NAME}
            requestMetadataView={tagView}
            actionRequest={actionRequest}
            onUpdate={onUpdate}
        />
    );
}
