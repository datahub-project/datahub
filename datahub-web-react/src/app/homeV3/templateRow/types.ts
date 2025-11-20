import { PageTemplateRowFragment } from '@graphql/template.generated';

export interface WrappedRow extends PageTemplateRowFragment {
    originRowIndex: number;
    rowIndex: number;
}
