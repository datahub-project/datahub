import analytics, { EntityActionType, EventType, ExternalLinkType } from '@src/app/analytics';
import { EntityType } from '@src/types.generated';

export function sendClickExternalLinkAnalytics(
    entityUrn: string,
    entityType: EntityType | undefined,
    linkType: ExternalLinkType,
) {
    analytics.event({
        type: EventType.EntityActionEvent,
        actionType: EntityActionType.ClickExternalUrl,
        entityUrn,
        entityType,
        externalLinkType: linkType,
    });
}
