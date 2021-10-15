import { RecommendationRenderType } from '../../types.generated';
import { useEntityRegistry } from '../useEntityRegistry';
import RecommendationRendererRegistry from './RecommendationRendererRegistry';
import { EntityListRenderer } from './renderer/EntityListRenderer';
import { PlatformListRenderer } from './renderer/PlatformListRenderer';

export function useRecommendationRenderer() {
    const entityRegistry = useEntityRegistry();
    const registry = new RecommendationRendererRegistry(entityRegistry);
    registry.register(RecommendationRenderType.EntityList, new EntityListRenderer(entityRegistry));
    registry.register(RecommendationRenderType.PlatformList, new PlatformListRenderer(entityRegistry));
    return registry;
}
