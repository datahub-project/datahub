/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { CUSTOM_MODULE_TYPES } from '@app/homeV3/modules/constants';

import { PageModuleFragment, PageTemplateFragment } from '@graphql/template.generated';

export function getCustomGlobalModules(globalTemplate: PageTemplateFragment | null) {
    const customGlobalModules: PageModuleFragment[] = [];
    globalTemplate?.properties?.rows?.forEach((row) => {
        row.modules.forEach((module) => {
            if (CUSTOM_MODULE_TYPES.includes(module.properties.type)) {
                customGlobalModules.push(module);
            }
        });
    });

    return customGlobalModules;
}
