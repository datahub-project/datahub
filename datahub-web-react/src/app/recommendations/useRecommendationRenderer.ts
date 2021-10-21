import { RecommendationRenderType } from '../../types.generated';
import RecommendationRendererRegistry from './RecommendationRendererRegistry';
import { EntityNameListRenderer } from './renderer/EntityNameListRenderer';
import { PlatformListRenderer } from './renderer/PlatformListRenderer';
import { TagSearchListRenderer } from './renderer/TagSearchListRenderer';

const registry = new RecommendationRendererRegistry();
registry.register(RecommendationRenderType.EntityNameList, new EntityNameListRenderer());
registry.register(RecommendationRenderType.PlatformSearchList, new PlatformListRenderer());
registry.register(RecommendationRenderType.TagSearchList, new TagSearchListRenderer());

export function useRecommendationRenderer() {
    return registry;
}
