import { Text } from '@components';
import React from 'react';
import { ActionRequest } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import { ContentWrapper, StyledLink } from './styledComponents';

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
            <StyledLink to={`/${entityRegistry.getPathName(requestTargetEntityType)}/${actionRequest.entity?.urn}`}>
                {requestTargetDisplayName}
            </StyledLink>
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
