import React from 'react';
import styled from 'styled-components';
import LaunchIcon from '@mui/icons-material/Launch';
import { Tooltip } from 'antd';
import { EntityType } from '../../../../types.generated';
import analytics, { EventType, EntityActionType } from '../../../analytics';
import { useEntityData } from '../../../entity/shared/EntityContext';
import { getPlatformName } from '../utils';
import { ActionMenuItem } from './styledComponents';

const GITHUB_LINK = 'github.com';
const GITHUB = 'GitHub';

export const PlatformIcon = styled.img<{ size?: number }>`
    max-height: ${(props) => (props.size ? props.size : 14)}px;
    width: auto;
    object-fit: contain;
    background-color: transparent;
`;

const StyledLaunchIcon = styled(LaunchIcon)`
    &&& {
        display: flex;
        font-size: 16px;
    }
`;

export default function ExternalUrlMenuAction() {
    const { urn: entityUrn, entityData, entityType } = useEntityData();
    const externalUrl = entityData?.externalUrl;
    const platformName = getPlatformName(entityData);

    if (!externalUrl) {
        return null;
    }

    function sendAnalytics() {
        analytics.event({
            type: EventType.EntityActionEvent,
            actionType: EntityActionType.ClickExternalUrl,
            entityType: entityType as EntityType,
            entityUrn,
        });
    }

    let displayedName = platformName;
    if (externalUrl?.toLocaleLowerCase().includes(GITHUB_LINK)) {
        displayedName = GITHUB;
    }

    return (
        <Tooltip title={`View in ${displayedName}`}>
            <ActionMenuItem
                key="external-url"
                href={externalUrl}
                target="_blank"
                rel="noreferrer noopener"
                onClick={sendAnalytics}
            >
                <StyledLaunchIcon />
            </ActionMenuItem>
        </Tooltip>
    );
}
