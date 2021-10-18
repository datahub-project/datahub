import React from 'react';
import { RecommendationContent, RecommendationRenderType } from '../../../types.generated';
import { PlatformList } from './component/PlatformList';
import { RecommendationDisplayType, RecommendationsRenderer } from './RecommendationsRenderer';

export class PlatformListRenderer implements RecommendationsRenderer {
    /* eslint-disable class-methods-use-this */
    renderRecommendation(
        _: string,
        _1: RecommendationRenderType,
        content: RecommendationContent[],
        _2: RecommendationDisplayType,
    ): JSX.Element {
        return <PlatformList content={content || []} />;
    }
}
