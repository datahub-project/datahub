import { RecommendationContent, RecommendationRenderType, ScenarioType } from '../../../types.generated';

/**
 * The display type that should be used when rendering the recommendation.
 */
export enum RecommendationDisplayType {
    DEFAULT,
    COMPACT,
}

/**
 * Base interface used for rendering recommendations pushed from DataHub's server.
 */
export interface RecommendationsRenderer {
    /**
     * Invoked when a recommendation should be rendered.
     */
    renderRecommendation(
        renderId: string,
        moduleId: string,
        scenarioType: ScenarioType,
        renderType: RecommendationRenderType,
        content: Array<RecommendationContent>,
        displayType: RecommendationDisplayType,
    ): JSX.Element;
}
