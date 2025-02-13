import { Text } from '@components';
import { REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';
import React from 'react';
import { Link } from 'react-router-dom';
import { ActionRequest } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import { ContentWrapper } from './styledComponents';

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
        <ContentWrapper>
            <Link
                to={`/${entityRegistry.getPathName(requestTargetEntityType)}/${actionRequest.entity?.urn}`}
                style={{ color: REDESIGN_COLORS.TITLE_PURPLE }}
            >
                {requestTargetDisplayName}
            </Link>
            {!!actionRequest.subResource && (
                <>
                    <Text color="gray" type="span">
                        {' '}
                        column{' '}
                        <Text color="gray" weight="bold" type="span">
                            {actionRequest.subResource}
                        </Text>
                    </Text>
                </>
            )}
        </ContentWrapper>
    );
}

export default RequestTargetEntityView;
