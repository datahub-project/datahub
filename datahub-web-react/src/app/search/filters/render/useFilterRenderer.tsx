import FilterRendererRegistry from '@app/search/filters/render/FilterRendererRegistry';
import { renderers as acrylRenderers } from '@app/search/filters/render/acrylRenderers';

/**
 * Configure the render registry.
 */
const RENDERERS = [...acrylRenderers];
const REGISTRY = new FilterRendererRegistry();
RENDERERS.forEach((renderer) => REGISTRY.register(renderer));

/**
 * Used to render custom filter views for a particular facet field.
 */
export const useFilterRendererRegistry = () => {
    return REGISTRY;
};
