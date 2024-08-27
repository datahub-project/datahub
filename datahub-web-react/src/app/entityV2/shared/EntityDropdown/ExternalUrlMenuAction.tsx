import React from 'react';
import styled from 'styled-components';
import LaunchIcon from '@mui/icons-material/Launch';
import { Tooltip } from 'antd';
import { EntityType } from '../../../../types.generated';
import analytics, { EventType, EntityActionType } from '../../../analytics';
import { useEntityData } from '../../../entity/shared/EntityContext';
import { getPlatformName } from '../utils';
import StyledExternalLink from '../links/StyledExternalLink';

const GITHUB_LINK = 'github.com';
const GITHUB = 'GitHub';

const Container = styled.div`
    margin: 0px 8px;
`;

export const PlatformIcon = styled.img<{ size?: number }>`
    max-height: ${(props) => (props.size ? props.size : 14)}px;
    width: auto;
    object-fit: contain;
    background-color: transparent;
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
            <Container>
                <StyledExternalLink url={externalUrl}>
                    <LaunchIcon
                        style={{
                            fontSize: '16px',
                        }}
                        onClick={sendAnalytics}
                    />
                    View in {displayedName}
                </StyledExternalLink>
            </Container>
        </Tooltip>
    );
}
