import React from 'react';
import { RecommendationContent, RecommendationRenderType } from '../../../types.generated';
import EntityRegistry from '../../entity/EntityRegistry';
import { RecommendationDisplayType, RecommendationsRenderer } from './RecommendationsRenderer';

export class PlatformListRenderer implements RecommendationsRenderer {
    entityRegistry;

    constructor(entityRegistry: EntityRegistry) {
        this.entityRegistry = entityRegistry;
    }

    renderRecommendation(
        moduleType: string,
        renderType: RecommendationRenderType,
        content: RecommendationContent[],
        displayType: RecommendationDisplayType,
    ): JSX.Element {
        console.log(moduleType);
        console.log(renderType);
        console.log(content);
        console.log(displayType);
        console.log(this.entityRegistry);
        return <>Not yet implemented!</>;
    }
}
