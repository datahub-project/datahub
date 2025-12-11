/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { SelectOption } from '@src/alchemy-components';
import { Entity } from '@src/types.generated';

export interface BaseEntitySelectOption extends SelectOption {
    entity: Entity; // stored in option to pass it in applied filters and forcibly show options from applied filters
    displayName: string; // for filtering by name
}
