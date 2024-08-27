import React from 'react';
import { RecommendationRenderProps } from '../types';
import { recommendationClickEvent } from '../util/recommendationClickEvent';
import { DomainSearchList } from './component/DomainSearchList';

export const DomainSearchListRenderer = ({
    renderId,
    moduleId,
    scenarioType,
    renderType,
    content,
}: RecommendationRenderProps) => {
    return (
        <DomainSearchList
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
