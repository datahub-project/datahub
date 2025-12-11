/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

export type RowSide = 'left' | 'right';

export interface ModulePositionInput {
    // When these fields are empty it means adding a module to the new row
    originRowIndex?: number; // Row index before wrapping
    rowIndex?: number; // Row index after wrapping
    rowSide?: RowSide;
    moduleIndex?: number; // Index of the module within the row (for precise removal)
}
