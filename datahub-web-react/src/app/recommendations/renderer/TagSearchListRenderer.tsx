import React from 'react';
import { RecommendationRenderProps } from '../types';
import { recommendationClickEvent } from '../util/recommendationClickEvent';
import { TagSearchList } from './component/TagSearchList';

export const TagSearchListRenderer = ({
    renderId,
    moduleId,
    scenarioType,
    renderType,
    content,
}: RecommendationRenderProps) => {
    return (
        <TagSearchList
            onClick={(index) =>
                recommendationClickEvent({
                    renderId: renderId.slice(),
                    moduleId,
                    scenarioType,
                    renderType,
                    index,
                })
            }
            content={content}
        />
    );
};
