import React from 'react';
import { Entity, RecommendationContent, RecommendationRenderType } from '../../../types.generated';
import { RecommendationDisplayType, RecommendationsRenderer } from './RecommendationsRenderer';
import { CompactEntityNameList } from './component/CompactEntityNameList';
import { EntityNameList } from './component/EntityNameList';

export class EntityNameListRenderer implements RecommendationsRenderer {
    /* eslint-disable class-methods-use-this */
    renderRecommendation(
        _: string,
        _1: RecommendationRenderType,
        content: RecommendationContent[],
        displayType: RecommendationDisplayType,
    ): JSX.Element {
        // todo: track clicks via module type.
        const entities = content.map((cnt) => cnt.entity).filter((entity) => entity !== undefined && entity !== null);
        console.log(content);
        return (
            <>
                {displayType === RecommendationDisplayType.COMPACT ? (
                    <CompactEntityNameList entities={entities as Array<Entity>} />
                ) : (
                    <EntityNameList entities={entities as Array<Entity>} />
                )}
            </>
        );
    }
}
