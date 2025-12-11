/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { SelectOption } from '@components/components/Select/types';

import { Entity } from '@src/types.generated';

export interface NestedSelectOption extends SelectOption {
    parentValue?: string;
    isParent?: boolean;
    entity?: Entity;
}
