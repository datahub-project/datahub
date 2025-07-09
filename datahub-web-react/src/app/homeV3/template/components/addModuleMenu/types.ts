import { DataHubPageModule, DataHubPageModuleType } from '@types';

export type AddModuleMenuHandlerInput = {
    module?: DataHubPageModule; // Fill in case of adding an existing module
    moduleType: DataHubPageModuleType;
};
