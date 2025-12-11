/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
