/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { EXACT_SEARCH_PREFIX, SEARCH_FOR_ENTITY_PREFIX } from '@app/searchV2/utils/constants';

export default function filterSearchQuery(v: string) {
    return (v && v.startsWith(SEARCH_FOR_ENTITY_PREFIX)) || v.startsWith(EXACT_SEARCH_PREFIX) ? v.split('__')[1] : v;
}
