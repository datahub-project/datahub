import { Typography } from 'antd';
import React, { useEffect, useMemo } from 'react';
import { v4 as uuidv4 } from 'uuid';
import { RecommendationModule as RecommendationModuleType, ScenarioType } from '../../types.generated';
import analytics, { EventType } from '../analytics';
import { renderTypeToRenderer } from './renderers';
import { RecommendationDisplayType } from './types';

type Props = {
    module: RecommendationModuleType;
    scenarioType: ScenarioType;
    displayType?: RecommendationDisplayType;
    showTitle?: boolean;
};

export const RecommendationModule = ({ module, scenarioType, displayType, showTitle }: Props) => {
    const finalDisplayType = displayType || RecommendationDisplayType.DEFAULT;
    const renderId = useMemo(() => uuidv4(), []);
    useEffect(() => {
        analytics.event({
            type: EventType.RecommendationImpressionEvent,
            moduleId: module.moduleId,
            renderType: module.renderType,
            scenarioType,
        });
    }, [module, scenarioType]);
    const RecommendationRenderer = renderTypeToRenderer.get(module.renderType);
    if (!RecommendationRenderer) {
        console.error(`Failed to find renderer corresponding to renderType ${module.renderType}`);
        return null;
    }
    return (
        <>
            {showTitle && <Typography.Title level={4}>{module.title}</Typography.Title>}
            <RecommendationRenderer
                renderId={renderId}
                moduleId={module.moduleId}
                scenarioType={scenarioType}
                renderType={module.renderType}
                content={module.content}
                displayType={finalDisplayType}
            />
        </>
    );
};
