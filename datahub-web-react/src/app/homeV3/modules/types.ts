/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { IconNames } from '@components';

import { DataHubPageModuleType } from '@types';

export type ModuleInfo = {
    key: string;
    urn?: string; // Filled in a case of working with an existing module (e.g. admin created modules)
    type: DataHubPageModuleType;
    name: string;
    description?: string;
    icon: IconNames;
};
