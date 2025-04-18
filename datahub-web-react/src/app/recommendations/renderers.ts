import React from 'react';
import { RecommendationRenderType } from '../../types.generated';
import { SearchQueryList } from './renderer/component/SearchQueryList';
import { EntityNameListRenderer } from './renderer/EntityNameListRenderer';
import { PlatformListRenderer } from './renderer/PlatformListRenderer';
import { TagSearchListRenderer } from './renderer/TagSearchListRenderer';
import { GlossaryTermSearchListRenderer } from './renderer/GlossaryTermSearchListRenderer';
import { RecommendationRenderProps } from './types';
import { DomainSearchListRenderer } from './renderer/DomainSearchListRenderer';

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
