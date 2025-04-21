import { LaunchOutlined } from '@mui/icons-material';
import React from 'react';
import styled from 'styled-components';

import analytics, { EntityActionType, EventType } from '@app/analytics';
import { ActionItem } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/actions/ActionItem';

import { Assertion, AssertionRunStatus, EntityType } from '@types';

const StyledLaunchOutlined = styled(LaunchOutlined)`
    && {
        display: flex;
        font-size: 16px;
    }
`;

type Props = {
    assertion: Assertion;
    isExpandedView?: boolean;
};

export const ExternalUrlAction = ({ assertion, isExpandedView = false }: Props) => {
    const platformName =
        assertion?.platform?.properties?.displayName || assertion?.platform?.name || 'external platform';
    const externalUrl =
        assertion?.info?.externalUrl ||
        (assertion.runEvents?.runEvents?.length &&
            assertion.runEvents.runEvents[0].status === AssertionRunStatus.Complete &&
            assertion.runEvents.runEvents[0].result?.externalUrl);

    if (!externalUrl) {
        return null;
    }

    const handleRedirect = () => {
        // Sending analytics data
        analytics.event({
            type: EventType.EntityActionEvent,
            actionType: EntityActionType.ClickExternalUrl,
            entityType: EntityType.Assertion,
            entityUrn: assertion.urn,
        });

        // Opening the URL in a new tab
        window.open(externalUrl, '_blank', 'noopener,noreferrer');
    };

    return (
        <ActionItem
            key="external-url"
            tip={`View in ${platformName}.`}
            onClick={handleRedirect}
            icon={<StyledLaunchOutlined />}
            isExpandedView={isExpandedView}
            actionName="View External platform"
        />
    );
};
