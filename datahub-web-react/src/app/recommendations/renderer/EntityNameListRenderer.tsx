import React from 'react';
import { Entity, RecommendationContent, RecommendationRenderType, ScenarioType } from '../../../types.generated';
import { RecommendationDisplayType, RecommendationsRenderer } from './RecommendationsRenderer';
import { CompactEntityNameList } from './component/CompactEntityNameList';
import { EntityNameList } from './component/EntityNameList';
import { recommendationClickEvent } from '../util/recommendationClickEvent';

export class EntityNameListRenderer implements RecommendationsRenderer {
    /* eslint-disable class-methods-use-this */
    renderRecommendation(
        renderId: string,
        moduleId: string,
        scenarioType: ScenarioType,
        renderType: RecommendationRenderType,
        content: RecommendationContent[],
        displayType: RecommendationDisplayType,
    ): JSX.Element {
        const entities = content.map((cnt) => cnt.entity).filter((entity) => entity !== undefined && entity !== null);
        const EntityNameListComponent =
            displayType === RecommendationDisplayType.COMPACT ? CompactEntityNameList : EntityNameList;
        return (
            <EntityNameListComponent
                onClick={(index) =>
                    recommendationClickEvent({
                        renderId,
                        moduleId,
                        scenarioType,
                        renderType,
                        index,
                    })
                }
                entities={entities as Array<Entity>}
            />
        );
    }
}
