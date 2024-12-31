import React from 'react';
import { EntityType } from '../../../types.generated';
import analytics, { EventType, EntityActionType } from '../../analytics';
import UrlButton from './UrlButton';

const GITHUB_LINK = 'github.com';
const GITHUB = 'GitHub';
const GITLAB_LINK = 'gitlab.com';
const GITLAB = 'GitLab';

interface Props {
    externalUrl: string;
    platformName?: string;
    entityUrn: string;
    entityType?: string;
}

export default function ExternalUrlButton({ externalUrl, platformName, entityType, entityUrn }: Props) {
    function sendAnalytics() {
        analytics.event({
            type: EventType.EntityActionEvent,
            actionType: EntityActionType.ClickExternalUrl,
            entityType: entityType as EntityType,
            entityUrn,
        });
    }

    let displayedName = platformName;
    if (externalUrl.toLocaleLowerCase().includes(GITHUB_LINK)) {
        displayedName = GITHUB;
    } else if (externalUrl.toLocaleLowerCase().includes(GITLAB_LINK)) {
        displayedName = GITLAB;
    }

    return (
        <UrlButton href={externalUrl} onClick={sendAnalytics}>
            {displayedName ? `View in ${displayedName}` : 'View link'}
        </UrlButton>
    );
}
