import React from 'react';
import { RecommendationContent, RecommendationRenderType, ScenarioType } from '../../../types.generated';
import { recommendationClickEvent } from '../util/recommendationClickEvent';
import { PlatformList } from './component/PlatformList';
import { RecommendationDisplayType, RecommendationsRenderer } from './RecommendationsRenderer';

export class PlatformListRenderer implements RecommendationsRenderer {
    /* eslint-disable class-methods-use-this */
    renderRecommendation(
        renderId: string,
        moduleId: string,
        scenarioType: ScenarioType,
        renderType: RecommendationRenderType,
        content: RecommendationContent[],
        _: RecommendationDisplayType,
    ): JSX.Element {
        return (
            <PlatformList
                onClick={(index) =>
                    recommendationClickEvent({
                        renderId: renderId.slice(),
                        moduleId,
                        scenarioType,
                        renderType,
                        index,
                    })
                }
                content={content || []}
            />
        );
    }
}
