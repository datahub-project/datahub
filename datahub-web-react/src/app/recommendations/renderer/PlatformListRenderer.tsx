import React from 'react';
import { RecommendationRenderProps } from '../types';
import { recommendationClickEvent } from '../util/recommendationClickEvent';
import { PlatformList } from './component/PlatformList';

export const PlatformListRenderer = ({
    renderId,
    moduleId,
    scenarioType,
    renderType,
    content,
}: RecommendationRenderProps) => {
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
};
