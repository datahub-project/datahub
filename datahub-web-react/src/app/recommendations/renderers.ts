import React from 'react';

import { DomainSearchListRenderer } from '@app/recommendations/renderer/DomainSearchListRenderer';
import { EntityNameListRenderer } from '@app/recommendations/renderer/EntityNameListRenderer';
import { GlossaryTermSearchListRenderer } from '@app/recommendations/renderer/GlossaryTermSearchListRenderer';
import { PlatformListRenderer } from '@app/recommendations/renderer/PlatformListRenderer';
import { TagSearchListRenderer } from '@app/recommendations/renderer/TagSearchListRenderer';
import { SearchQueryList } from '@app/recommendations/renderer/component/SearchQueryList';
import { RecommendationRenderProps } from '@app/recommendations/types';

import { RecommendationRenderType } from '@types';

/**
 * Renderers that are responsible for rendering recommendations of a particular RenderType!
 */
const renderers: Array<{ renderType: RecommendationRenderType; renderer: React.FC<RecommendationRenderProps> }> = [
    {
        renderType: RecommendationRenderType.EntityNameList,
        renderer: EntityNameListRenderer,
    },
    {
        renderType: RecommendationRenderType.PlatformSearchList,
        renderer: PlatformListRenderer,
    },
    {
        renderType: RecommendationRenderType.TagSearchList,
        renderer: TagSearchListRenderer,
    },
    {
        renderType: RecommendationRenderType.SearchQueryList,
        renderer: SearchQueryList,
    },
    {
        renderType: RecommendationRenderType.GlossaryTermSearchList,
        renderer: GlossaryTermSearchListRenderer,
    },
    {
        renderType: RecommendationRenderType.DomainSearchList,
        renderer: DomainSearchListRenderer,
    },
];

export const renderTypeToRenderer: Map<RecommendationRenderType, React.FC<RecommendationRenderProps>> = new Map<
    RecommendationRenderType,
    React.FC<RecommendationRenderProps>
>();

renderers.forEach((entry) => {
    renderTypeToRenderer.set(entry.renderType, entry.renderer);
});
