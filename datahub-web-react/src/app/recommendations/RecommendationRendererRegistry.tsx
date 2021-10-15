import { RecommendationContent, RecommendationRenderType } from '../../types.generated';
import EntityRegistry from '../entity/EntityRegistry';
import { RecommendationsRenderer, RecommendationDisplayType } from './renderer/RecommendationsRenderer';

function validatedGet<K, V>(key: K, map: Map<K, V>): V {
    if (map.has(key)) {
        return map.get(key) as V;
    }
    throw new Error(`Unrecognized key ${key} provided in map ${JSON.stringify(map)}`);
}

/**
 * Serves as a singleton registry for all recommendation renderers
 */
export default class RecommendationRendererRegistry {
    entityRegistry;

    renderers: Array<RecommendationsRenderer> = new Array<RecommendationsRenderer>();

    renderTypeToRenderer: Map<RecommendationRenderType, RecommendationsRenderer> = new Map<
        RecommendationRenderType,
        RecommendationsRenderer
    >();

    constructor(entityRegistry: EntityRegistry) {
        this.entityRegistry = entityRegistry;
    }

    register(renderType: RecommendationRenderType, renderer: RecommendationsRenderer) {
        this.renderers.push(renderer);
        this.renderTypeToRenderer.set(renderType, renderer);
    }

    renderRecommendation(
        moduleType: string,
        renderType: RecommendationRenderType,
        content: Array<RecommendationContent>,
        displayType: RecommendationDisplayType,
    ): JSX.Element {
        const renderer = validatedGet(renderType, this.renderTypeToRenderer);
        return renderer.renderRecommendation(moduleType, renderType, content, displayType);
    }
}
