import { RecommendationContent, RecommendationRenderType, ScenarioType } from '../../types.generated';
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
    renderers: Array<RecommendationsRenderer> = new Array<RecommendationsRenderer>();

    renderTypeToRenderer: Map<RecommendationRenderType, RecommendationsRenderer> = new Map<
        RecommendationRenderType,
        RecommendationsRenderer
    >();

    register(renderType: RecommendationRenderType, renderer: RecommendationsRenderer) {
        this.renderers.push(renderer);
        this.renderTypeToRenderer.set(renderType, renderer);
    }

    renderRecommendation(
        renderId: string,
        moduleId: string,
        scenarioType: ScenarioType,
        renderType: RecommendationRenderType,
        content: Array<RecommendationContent>,
        displayType: RecommendationDisplayType,
    ): JSX.Element {
        const renderer = validatedGet(renderType, this.renderTypeToRenderer);
        return renderer.renderRecommendation(renderId, moduleId, scenarioType, renderType, content, displayType);
    }
}
