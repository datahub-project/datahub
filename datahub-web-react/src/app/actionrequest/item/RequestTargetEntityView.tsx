import { Typography } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import { ActionRequest } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';

interface Props {
    actionRequest: ActionRequest;
}

function RequestTargetEntityView({ actionRequest }: Props) {
    const entityRegistry = useEntityRegistry();

    const requestTargetEntityType = actionRequest.entity?.type;
    if (!requestTargetEntityType) return null;

    const requestTargetDisplayName =
        requestTargetEntityType && entityRegistry.getDisplayName(requestTargetEntityType, actionRequest.entity);

    return (
        <>
            <Link to={`/${entityRegistry.getPathName(requestTargetEntityType)}/${actionRequest.entity?.urn}`}>
                {requestTargetDisplayName}
            </Link>
            {!!actionRequest.subResource && (
                <>
                    {' '}
                    column <Typography.Text style={{ fontWeight: 'bold' }}>{actionRequest.subResource}</Typography.Text>
                </>
            )}
        </>
    );
}

export default RequestTargetEntityView;
