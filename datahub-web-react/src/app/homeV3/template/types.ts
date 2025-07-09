export type RowSide = 'left' | 'right';

export interface AddModuleHandlerInput {
    // When these fields are empty it means adding a module to the new row
    rowIndex?: number;
    rowSide?: RowSide;
}
