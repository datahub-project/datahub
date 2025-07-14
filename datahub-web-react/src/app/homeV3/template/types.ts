export type RowSide = 'left' | 'right';

export interface ModulePositionInput {
    // When these fields are empty it means adding a module to the new row
    originRowIndex?: number; // Row index before wrapping
    rowIndex?: number; // Row index after wrapping
    rowSide?: RowSide;
    moduleIndex?: number; // Index of the module within the row (for precise removal)
}

export interface CreateNewModuleInput {
    name: string;
    description?: string;
}
