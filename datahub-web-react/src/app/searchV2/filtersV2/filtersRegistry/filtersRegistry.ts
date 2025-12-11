/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { FieldName, FilterComponent } from '@app/searchV2/filtersV2/types';

export default class FiltersRegistry {
    registry: Map<string, FilterComponent> = new Map<FieldName, FilterComponent>();

    register(fieldName: FieldName, component: FilterComponent) {
        this.registry.set(fieldName, component);
    }

    has(fieldName: FieldName): boolean {
        return this.registry.has(fieldName);
    }

    get(fieldName: FieldName): FilterComponent | undefined {
        return this.registry.get(fieldName);
    }
}
