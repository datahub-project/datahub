import { Typography } from 'antd';
import React from 'react';
import { RecommendationModule as RecommendationModuleType } from '../../types.generated';
import { RecommendationDisplayType } from './renderer/RecommendationsRenderer';
import { useRecommendationRenderer } from './useRecommendationRenderer';

type Props = {
    module: RecommendationModuleType;
    displayType?: RecommendationDisplayType;
    showTitle?: boolean;
};

export const RecommendationModule = ({ module, displayType, showTitle }: Props) => {
    const finalDisplayType = displayType || RecommendationDisplayType.DEFAULT; // Fallback to default item size if not provided.
    const recommendationRenderer = useRecommendationRenderer();
    return (
        <>
            {showTitle && <Typography.Title level={4}>{module.title}</Typography.Title>}
            {recommendationRenderer.renderRecommendation(
                module.moduleId,
                module.renderType,
                module.content,
                finalDisplayType,
            )}
        </>
    );
};
