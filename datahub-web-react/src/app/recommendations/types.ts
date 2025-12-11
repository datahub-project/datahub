/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { RecommendationContent, RecommendationRenderType, ScenarioType } from '@types';

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
