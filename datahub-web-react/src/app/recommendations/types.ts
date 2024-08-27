import { RecommendationContent, RecommendationRenderType, ScenarioType } from '../../types.generated';

/**
 * The display type that should be used when rendering the recommendation.
 */
export enum RecommendationDisplayType {
    DEFAULT,
    COMPACT,
}

/**
 * Props passed to every recommendation renderer
 */
export type RecommendationRenderProps = {
    renderId: string;
    moduleId: string;
    scenarioType: ScenarioType;
    renderType: RecommendationRenderType;
    content: Array<RecommendationContent>;
    displayType: RecommendationDisplayType;
};
