import { Typography } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';
import { ActionRequest, EntityType } from '../../../types.generated';
import { CustomAvatar } from '../../shared/avatar';
import { useEntityRegistry } from '../../useEntityRegistry';

const AuthorView = styled.span`
    margin-left: 4px;
`;

const AuthorText = styled(Typography.Text)`
    margin-left: 2px;
`;

interface Props {
    actionRequest: ActionRequest;
}

function CreatedByView({ actionRequest }: Props) {
    const entityRegistry = useEntityRegistry();

    const createdBy = actionRequest.created.actor;
    const createdByDisplayName =
        (createdBy && entityRegistry.getDisplayName(EntityType.CorpUser, createdBy)) || 'Anonymous';
    const createdByDisplayImage = createdBy && createdBy.editableInfo?.pictureLink;
    const createdByProfileUrl = `/${entityRegistry.getPathName(EntityType.CorpUser)}/${createdBy?.urn}`;

    return (
        <AuthorView>
            <CustomAvatar
                name={createdByDisplayName}
                url={createdByProfileUrl}
                photoUrl={createdByDisplayImage || undefined}
            />
            <Link to={createdByProfileUrl}>
                <AuthorText strong>{createdByDisplayName}</AuthorText>
            </Link>
        </AuthorView>
    );
}

export default CreatedByView;
