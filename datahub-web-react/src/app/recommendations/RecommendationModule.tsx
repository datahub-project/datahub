/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Typography } from 'antd';
import React, { useEffect, useMemo } from 'react';
import { v4 as uuidv4 } from 'uuid';

import analytics, { EventType } from '@app/analytics';
import { renderTypeToRenderer } from '@app/recommendations/renderers';
import { RecommendationDisplayType } from '@app/recommendations/types';

import { RecommendationModule as RecommendationModuleType, ScenarioType } from '@types';

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
