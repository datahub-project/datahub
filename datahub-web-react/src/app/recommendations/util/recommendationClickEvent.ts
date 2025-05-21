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
