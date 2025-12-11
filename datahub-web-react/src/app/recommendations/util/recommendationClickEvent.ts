/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import analytics, { EventType } from '@app/analytics';

import { RecommendationRenderType, ScenarioType } from '@types';

export function recommendationClickEvent({
    renderId,
    moduleId,
    renderType,
    scenarioType,
    index,
}: {
    renderId: string;
    moduleId: string;
    renderType: RecommendationRenderType;
    scenarioType: ScenarioType;
    index: number;
}) {
    analytics.event({
        type: EventType.RecommendationClickEvent,
        renderId,
        moduleId,
        renderType,
        scenarioType,
        index,
    });
}
