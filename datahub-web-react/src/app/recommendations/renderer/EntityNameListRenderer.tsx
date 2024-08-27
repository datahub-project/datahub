import React from 'react';
import { Entity } from '../../../types.generated';
import { CompactEntityNameList } from './component/CompactEntityNameList';
import { EntityNameList } from './component/EntityNameList';
import { recommendationClickEvent } from '../util/recommendationClickEvent';
import { RecommendationDisplayType, RecommendationRenderProps } from '../types';

export const EntityNameListRenderer = ({
    renderId,
    moduleId,
    scenarioType,
    renderType,
    content,
    displayType,
}: RecommendationRenderProps) => {
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
};
