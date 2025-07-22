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
        description: getModuleDescription(module),
        icon: getModuleIcon(module),
    };
}
