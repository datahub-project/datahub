/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import FilterRendererRegistry from '@app/search/filters/render/FilterRendererRegistry';
import { HasActiveIncidentsRenderer } from '@app/search/filters/render/incident/HasActiveIncidentsRenderer';
import { HasSiblingsRenderer } from '@app/search/filters/render/siblings/HasSiblingsRenderer';

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
