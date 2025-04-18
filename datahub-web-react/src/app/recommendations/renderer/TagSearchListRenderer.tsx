import React from 'react';

import { TagSearchList } from '@app/recommendations/renderer/component/TagSearchList';
import { RecommendationRenderProps } from '@app/recommendations/types';
import { recommendationClickEvent } from '@app/recommendations/util/recommendationClickEvent';

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
