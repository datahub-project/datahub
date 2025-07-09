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

export type ModulesAvailableToAdd = {
    customModules: ModuleInfo[];
    customLargeModules: ModuleInfo[];
    adminCreatedModules: ModuleInfo[];
};
