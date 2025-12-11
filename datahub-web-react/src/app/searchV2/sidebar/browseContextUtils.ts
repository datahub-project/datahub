/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { ENTITY_SUB_TYPE_FILTER_NAME } from '@app/searchV2/utils/constants';

import { FacetFilterInput } from '@types';

export function getEntitySubtypeFiltersForEntity(entityType: string, existingFilters: FacetFilterInput[]) {
    return existingFilters
        .find((f) => f.field === ENTITY_SUB_TYPE_FILTER_NAME)
        ?.values?.filter((value) => value.includes(entityType));
}
