import { Typography } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';

import { CustomAvatar } from '@app/shared/avatar';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { ActionRequest, EntityType } from '@types';

const AuthorView = styled.span`
    margin-left: 4px;
`;

const AuthorText = styled(Typography.Text)`
    margin-left: 2px;
    font-weight: 700;
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
                <AuthorText>{createdByDisplayName}</AuthorText>
            </Link>
        </AuthorView>
    );
}

export default CreatedByView;
