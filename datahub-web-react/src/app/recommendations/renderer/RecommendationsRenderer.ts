import { RecommendationContent, RecommendationRenderType } from '../../../types.generated';

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
        moduleId: string,
        renderType: RecommendationRenderType,
        content: Array<RecommendationContent>,
        displayType: RecommendationDisplayType,
    ): JSX.Element;
}
