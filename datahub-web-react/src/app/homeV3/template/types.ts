import { DataHubPageModule, DataHubPageModuleType } from '@types';

export type RowSide = 'left' | 'right';

export interface AddModuleInput {
    module?: DataHubPageModule; // Fill in case of adding an existing module
    moduleType: DataHubPageModuleType;

    // When these fields are empty it means adding a module to the new row
    rowIndex?: number;
    rowSide?: RowSide;
}
