import React from 'react';

import { GlossaryTermSearchList } from '@app/recommendations/renderer/component/GlossaryTermSearchList';
import { RecommendationRenderProps } from '@app/recommendations/types';
import { recommendationClickEvent } from '@app/recommendations/util/recommendationClickEvent';

export const GlossaryTermSearchListRenderer = ({
    renderId,
    moduleId,
    scenarioType,
    renderType,
    content,
}: RecommendationRenderProps) => {
    return (
        <GlossaryTermSearchList
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
