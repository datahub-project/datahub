import FilterRendererRegistry from './FilterRendererRegistry';

/**
 * Configure the render registry.
 */
const RENDERERS = [
    /* Renderers will be registered here  */
];
const REGISTRY = new FilterRendererRegistry();
RENDERERS.forEach((renderer) => REGISTRY.register(renderer));

/**
 * Used to render custom filter views for a particular facet field.
 */
export const useFilterRendererRegistry = () => {
    return REGISTRY;
};
