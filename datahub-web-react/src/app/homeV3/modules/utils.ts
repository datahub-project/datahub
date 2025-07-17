import { IconNames } from '@components';

import { DEFAULT_MODULE_ICON, MODULE_TYPE_TO_DESCRIPTION, MODULE_TYPE_TO_ICON } from '@app/homeV3/modules/constants';
import { ModuleInfo } from '@app/homeV3/modules/types';

import { DataHubPageModule, DataHubPageModuleType } from '@types';

export function getModuleType(module: DataHubPageModule): DataHubPageModuleType {
    return module.properties.type;
}

export function getModuleIcon(module: DataHubPageModule): IconNames {
    return MODULE_TYPE_TO_ICON.get(getModuleType(module)) ?? DEFAULT_MODULE_ICON;
}

export function getModuleName(module: DataHubPageModule): string {
    return module.properties.name;
}

export function getModuleDescription(module: DataHubPageModule): string | undefined {
    // TODO: implement getting of the correct description
    return MODULE_TYPE_TO_DESCRIPTION.get(getModuleType(module));
}

export function convertModuleToModuleInfo(module: DataHubPageModule): ModuleInfo {
    return {
        urn: module.urn,
        key: module.urn,
        type: getModuleType(module),
        name: getModuleName(module),
        description: getModuleDescription(module),
        icon: getModuleIcon(module),
    };
}
