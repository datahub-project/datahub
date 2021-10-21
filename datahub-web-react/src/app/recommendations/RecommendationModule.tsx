import { Typography } from 'antd';
import React, { useMemo } from 'react';
import { v4 as uuidv4 } from 'uuid';
import { RecommendationModule as RecommendationModuleType, ScenarioType } from '../../types.generated';
import analytics, { EventType } from '../analytics';
import { RecommendationDisplayType } from './renderer/RecommendationsRenderer';
import { useRecommendationRenderer } from './useRecommendationRenderer';

type Props = {
    module: RecommendationModuleType;
    scenarioType: ScenarioType;
    displayType?: RecommendationDisplayType;
    showTitle?: boolean;
};

export const RecommendationModule = ({ module, scenarioType, displayType, showTitle }: Props) => {
    const finalDisplayType = displayType || RecommendationDisplayType.DEFAULT; // Fallback to default item size if not provided.
    const recommendationRenderer = useRecommendationRenderer();
    const renderId = useMemo(() => uuidv4(), []);
    analytics.event({
        type: EventType.RecommendationImpressionEvent,
        renderId,
        moduleId: module.moduleId,
        renderType: module.renderType,
        scenarioType,
    });
    return (
        <>
            {showTitle && <Typography.Title level={4}>{module.title}</Typography.Title>}
            {recommendationRenderer.renderRecommendation(
                renderId,
                module.moduleId,
                scenarioType,
                module.renderType,
                module.content,
                finalDisplayType,
            )}
        </>
    );
};
