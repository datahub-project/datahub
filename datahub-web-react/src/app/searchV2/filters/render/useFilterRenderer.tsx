import { renderers as acrylRenderers } from './acrylRenderers';
import FilterRendererRegistry from './FilterRendererRegistry';

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
