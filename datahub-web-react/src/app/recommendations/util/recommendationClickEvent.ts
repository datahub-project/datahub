import { RecommendationRenderType, ScenarioType } from '../../../types.generated';
import analytics, { EventType } from '../../analytics';

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
