import React from 'react';
import { RecommendationRenderProps } from '../types';
import { recommendationClickEvent } from '../util/recommendationClickEvent';
import { SearchQueryList } from './component/SearchQueryList';

export const SearchQueryListRenderer = ({
    renderId,
    moduleId,
    scenarioType,
    renderType,
    content,
}: RecommendationRenderProps) => {
    return (
        <SearchQueryList
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
