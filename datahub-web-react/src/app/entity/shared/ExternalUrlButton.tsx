import React from 'react';
import { message } from 'antd';
import { EntityType } from '../../../types.generated';
import analytics, { EventType, EntityActionType } from '../../analytics';
import UrlButton from './UrlButton';

const GITHUB = 'github.com';
const ALLOWED_GITHUB_HOSTS = ['github.com', 'www.github.com'];

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
    try {
      const host = new URL(externalUrl).host;
      if (ALLOWED_GITHUB_HOSTS.includes(host.toLocaleLowerCase())) {
          displayedName = GITHUB;
      }
    } catch(e: any) {
        message.error({ content: `Not a valid URL! \n ${e?.message || ''}`, duration: 3 });
    }

    return (
        <UrlButton href={externalUrl} onClick={sendAnalytics}>
            {displayedName ? `View in ${displayedName}` : 'View link'}
        </UrlButton>
    );
}
