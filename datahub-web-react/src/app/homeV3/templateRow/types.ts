import { DataHubPageTemplateRow } from '@types';

export interface WrappedRow extends DataHubPageTemplateRow {
    originRowIndex: number;
    rowIndex: number;
}
