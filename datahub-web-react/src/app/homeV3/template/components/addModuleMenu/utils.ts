import { IconNames } from '@components';

import {
    DEFAULT_MODULE_ICON,
    MODULE_TYPE_TO_DESCRIPTION_OVERRIDE,
    MODULE_TYPE_TO_ICON,
} from '@app/homeV3/template/components/addModuleMenu/constants';

import { DataHubPageModule, DataHubPageModuleType } from '@types';

export function getModuleIcon(module: DataHubPageModule): IconNames {
    return getModuleIconByType(module.properties.type);
}

export function getModuleIconByType(moduleType: DataHubPageModuleType): IconNames {
    return MODULE_TYPE_TO_ICON.get(moduleType) ?? DEFAULT_MODULE_ICON;
}

export function getModuleTitle(module: DataHubPageModule): string {
    return module.properties.name;
}

export function getModuleDescription(module: DataHubPageModule): string | undefined {
    // TODO: implement getting of the correct description
    return MODULE_TYPE_TO_DESCRIPTION_OVERRIDE.get(module.properties.type);
}
