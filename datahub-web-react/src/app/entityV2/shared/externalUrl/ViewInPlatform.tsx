import { EntityActionType, EventType } from '@app/analytics';
import analytics from '@app/analytics/analytics';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import LaunchIcon from '@mui/icons-material/Launch';
import { EntityType } from '@types';
import React from 'react';
import styled from 'styled-components';

const GITHUB_LINK = 'github.com';
const GITHUB_NAME = 'GitHub';
const GITLAB_LINK = 'gitlab.com';
const GITLAB_NAME = 'GitLab';

const Link = styled.a`
    display: flex;
    align-items: center;
    gap: 5px;

    border-radius: 4px;
    padding: 4px 6px;
    margin: 0px 4px;

    background: ${REDESIGN_COLORS.LIGHT_TEXT_DARK_BACKGROUND};
    color: ${REDESIGN_COLORS.TITLE_PURPLE};
`;

const IconWrapper = styled.span`
    display: flex;
    align-items: center;
    font-size: ${4 / 3}em;
`;

interface Props {
    urn: string;
    entityType?: EntityType | null;
    platform?: string;
    externalUrl?: string | null;
    className?: string;
}

export default function ViewInPlatform({ urn, entityType, platform, externalUrl, className }: Props) {
    if (!externalUrl) return null;

    function sendAnalytics() {
        analytics.event({
            type: EventType.EntityActionEvent,
            actionType: EntityActionType.ClickExternalUrl,
            entityUrn: urn,
            entityType: entityType ?? undefined,
        });
    }

    let displayedName = platform;
    if (externalUrl?.toLocaleLowerCase().includes(GITHUB_LINK)) {
        displayedName = GITHUB_NAME;
    } else if (externalUrl?.toLocaleLowerCase().includes(GITLAB_LINK)) {
        displayedName = GITLAB_NAME;
    }

    return (
        <Link href={externalUrl} target="_blank" onClick={sendAnalytics} className={className}>
            <IconWrapper>
                <LaunchIcon fontSize="inherit" />
            </IconWrapper>
            View in {displayedName || 'source'}
        </Link>
    );
}
