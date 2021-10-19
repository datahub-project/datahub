import React from 'react';
import { RecommendationContent, RecommendationRenderType } from '../../../types.generated';
import { TagSearchList } from './component/TagSearchList';
import { RecommendationDisplayType, RecommendationsRenderer } from './RecommendationsRenderer';

export class TagSearchListRenderer implements RecommendationsRenderer {
    /* eslint-disable class-methods-use-this */
    renderRecommendation(
        _: string,
        _1: RecommendationRenderType,
        content: RecommendationContent[],
        _2: RecommendationDisplayType,
    ): JSX.Element {
        return <TagSearchList content={content} />;
    }
}
