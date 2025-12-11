/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import FilterRendererRegistry from '@app/searchV2/filters/render/FilterRendererRegistry';
import { renderers as acrylRenderers } from '@app/searchV2/filters/render/acrylRenderers';

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
