import React from 'react';

import { CompactEntityNameList } from '@app/recommendations/renderer/component/CompactEntityNameList';
import { EntityNameList } from '@app/recommendations/renderer/component/EntityNameList';
import { RecommendationDisplayType, RecommendationRenderProps } from '@app/recommendations/types';
import { recommendationClickEvent } from '@app/recommendations/util/recommendationClickEvent';

import { Entity } from '@types';

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
