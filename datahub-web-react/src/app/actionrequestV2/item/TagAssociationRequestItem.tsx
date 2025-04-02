import { StyledTag } from '@src/app/entity/shared/components/styled/StyledTag';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { ActionRequest, ActionRequestResult, EntityType } from '@src/types.generated';
import React from 'react';
import { Link } from 'react-router-dom';
import AddContentView from './AddContentView';

interface Props {
    actionRequest: ActionRequest;
}

const TagAssociationRequestItem = ({ actionRequest }: Props) => {
    const entityRegistry = useEntityRegistry();

    const tag = actionRequest.params?.tagProposal?.tag || actionRequest.params?.tagProposal?.tags?.[0];
    const tagView = tag && (
        <Link to={`/${entityRegistry.getPathName(EntityType.Tag)}/${tag.urn}`}>
            <StyledTag
                $color={tag?.properties?.colorHex}
                $colorHash={tag.urn}
                style={{ marginRight: 2, marginLeft: 2 }}
                $isApproved={actionRequest.result === ActionRequestResult.Accepted}
            >
                {entityRegistry.getDisplayName(EntityType.Tag, tag)}
            </StyledTag>
        </Link>
    );

    return <AddContentView requestMetadataViews={[{ primary: tagView }]} actionRequest={actionRequest} />;
};

export default TagAssociationRequestItem;
