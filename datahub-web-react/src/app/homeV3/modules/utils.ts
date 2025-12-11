/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { IconNames } from '@components';

import { DEFAULT_MODULE_ICON, MODULE_TYPE_TO_DESCRIPTION, MODULE_TYPE_TO_ICON } from '@app/homeV3/modules/constants';
import { ModuleInfo } from '@app/homeV3/modules/types';

import { PageModuleFragment } from '@graphql/template.generated';
import { DataHubPageModuleType } from '@types';

export function getModuleType(module: PageModuleFragment): DataHubPageModuleType {
    return module.properties.type;
}

export function getModuleIcon(module: PageModuleFragment): IconNames {
    return MODULE_TYPE_TO_ICON.get(getModuleType(module)) ?? DEFAULT_MODULE_ICON;
}

export function getModuleName(module: PageModuleFragment): string {
    return module.properties.name;
}

export function getModuleDescription(module: PageModuleFragment): string | undefined {
    // TODO: implement getting of the correct description
    return MODULE_TYPE_TO_DESCRIPTION.get(getModuleType(module));
}

export function convertModuleToModuleInfo(module: PageModuleFragment): ModuleInfo {
    return {
        urn: module.urn,
        key: module.urn,
        type: getModuleType(module),
        name: getModuleName(module),
        icon: getModuleIcon(module),
    };
}
