/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { FilterRenderer } from '@app/searchV2/filters/render/FilterRenderer';
import { HasFailingAssertionsRenderer } from '@app/searchV2/filters/render/assertion/HasFailingAssertionsRenderer';
import { DeprecationRenderer } from '@app/searchV2/filters/render/deprecation/DeprecationRenderer';
import { HasActiveIncidentsRenderer } from '@app/searchV2/filters/render/incident/HasActiveIncidentsRenderer';
import { HasSiblingsRenderer } from '@app/searchV2/filters/render/siblings/HasSiblingsRenderer';

export const renderers: Array<FilterRenderer> = [
    new HasFailingAssertionsRenderer(),
    new HasActiveIncidentsRenderer(),
    new HasSiblingsRenderer(),
    new DeprecationRenderer(),
];
