import { Text } from '@components';
import React from 'react';

import { ContentWrapper, StyledLink } from '@app/actionrequestV2/item/styledComponents';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { ActionRequest } from '@types';

interface Props {
    actionRequest: ActionRequest;
}

function RequestTargetEntityView({ actionRequest }: Props) {
    const entityRegistry = useEntityRegistry();
    const { theme } = useCustomTheme();

    const requestTargetEntityType = actionRequest.entity?.type;
    if (!requestTargetEntityType) return null;

    const requestTargetDisplayName =
        requestTargetEntityType && entityRegistry.getDisplayName(requestTargetEntityType, actionRequest.entity);

    return (
        <ContentWrapper>
            <StyledLink
                to={`/${entityRegistry.getPathName(requestTargetEntityType)}/${actionRequest.entity?.urn}`}
                style={{ color: getColor('primary', 500, theme) }}
            >
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
