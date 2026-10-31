import { HasActiveIncidentsRenderer } from '@app/search/filters/render/incident/HasActiveIncidentsRenderer';
import { HasSiblingsRenderer } from '@app/search/filters/render/siblings/HasSiblingsRenderer';
import FilterRendererRegistry from '@app/searchV2/filters/render/FilterRendererRegistry';

/**
 * Configure the render registry.
 */
const RENDERERS = [new HasActiveIncidentsRenderer(), new HasSiblingsRenderer()];
const REGISTRY = new FilterRendererRegistry();
RENDERERS.forEach((renderer) => REGISTRY.register(renderer));

/**
 * Used to render custom filter views for a particular facet field.
 */
export const useFilterRendererRegistry = () => {
    return REGISTRY;
};
