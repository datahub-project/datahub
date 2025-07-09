import { useMemo } from 'react';

import {
    ADD_MODULE_MENU_SECTION_CUSTOM_LARGE_MODULE_TYPES,
    ADD_MODULE_MENU_SECTION_CUSTOM_MODULE_TYPES,
    DEFAULT_MODULES,
} from '@app/homeV3/modules/constants';
import { ModulesAvailableToAdd } from '@app/homeV3/modules/types';
import { PageModuleFragment } from '@graphql/template.generated';

import { DataHubPageModuleType, EntityType, PageModuleScope } from '@types';

// TODO: Mocked default modules (should be replaced with the real calling of endpoint once it implemented)
export const MOCKED_ADMIN_CREATED_MODULES: PageModuleFragment[] = [
    {
        urn: 'urn:li:dataHubPageModule:link_admin_1',
        type: EntityType.DatahubPageModule,
        properties: {
            created: {
                time: 1752056099724,
            },
            lastModified: {
                time: 1752056099724,
            },
            name: 'Link 1 (example)',
            type: DataHubPageModuleType.Link,
            visibility: {
                scope: PageModuleScope.Global,
            },
            params: {},
        },
    },
    {
        urn: 'urn:li:dataHubPageModule:link_admin_2',
        type: EntityType.DatahubPageModule,
        properties: {
            created: {
                time: 1752056099724,
            },
            lastModified: {
                time: 1752056099724,
            },
            name: 'Link 2 (example)',
            type: DataHubPageModuleType.Link,
            visibility: {
                scope: PageModuleScope.Global,
            },
            params: {},
        },
    },
];

export default function useModulesAvailableToAdd(): ModulesAvailableToAdd {
    // TODO:: here we have to add logic with getting available modules
    return useMemo(() => {
        const customModules = DEFAULT_MODULES.filter((module) =>
            ADD_MODULE_MENU_SECTION_CUSTOM_MODULE_TYPES.includes(module.type),
        );
        const customLargeModules = DEFAULT_MODULES.filter((module) =>
            ADD_MODULE_MENU_SECTION_CUSTOM_LARGE_MODULE_TYPES.includes(module.type),
        );
        const adminCreatedModules = MOCKED_ADMIN_CREATED_MODULES;

        return {
            customModules,
            customLargeModules,
            adminCreatedModules,
        };
    }, []);
}
