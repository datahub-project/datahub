import React from 'react';
import { RecommendationRenderProps } from '../types';
import { recommendationClickEvent } from '../util/recommendationClickEvent';
import { GlossaryTermSearchList } from './component/GlossaryTermSearchList';

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
