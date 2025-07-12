import { ModuleInfo } from '@app/homeV3/modules/types';

export type RowSide = 'left' | 'right';

export interface AddModuleHandlerInput {
    module: ModuleInfo;

    // When these fields are empty it means adding a module to the new row
    originRowIndex?: number; // Row index before wrapping
    rowIndex?: number; // Row index after wrapping
    rowSide?: RowSide;
}
