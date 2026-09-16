import { WrappedRow } from '@app/homeV3/templateRow/types';

import { PageTemplateRowFragment } from '@graphql/template.generated';

const DEFAULT_ROW_MAX_SIZE = 3;

export function wrapRows(rows: PageTemplateRowFragment[], chunkSize = DEFAULT_ROW_MAX_SIZE): WrappedRow[] {
    const result: WrappedRow[] = [];
    let globalRowIndex = 0;

    rows.forEach((row, originRowIndex) => {
        const { modules } = row;

        for (let i = 0; i < modules.length; i += chunkSize) {
            const chunk = modules.slice(i, i + chunkSize);
            result.push({
                originRowIndex,
                rowIndex: globalRowIndex++,
                modules: chunk,
            });
        }
    });

    return result;
}
