import { Text } from '@components';
import React from 'react';

import { ContentWrapper, StyledLink } from '@app/actionrequestV2/item/styledComponents';
import { downgradeV2FieldPath } from '@app/entityV2/dataset/profile/schema/utils/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { ActionRequest } from '@types';

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
                            {downgradeV2FieldPath(actionRequest.subResource)}
                        </Text>
                    </Text>
                </>
            )}
        </ContentWrapper>
    );
}

export default RequestTargetEntityView;
