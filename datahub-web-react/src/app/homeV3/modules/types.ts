import { IconNames } from '@components';

import { DataHubPageModuleType } from '@types';
import { PageModuleFragment } from '@graphql/template.generated';

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
    adminCreatedModules: PageModuleFragment[]; // Full module fragments for admin-created modules
};
