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

/**
 * Rows to draw: a row whose modules are all hidden takes no space. Rows keep their own rowIndex,
 * so positions still match the template. A row with no modules at all is kept.
 */
export function getVisibleRows(rows: WrappedRow[], isModuleHidden: (module: WrappedRow['modules'][0]) => boolean) {
    return rows.filter((row) => row.modules.length === 0 || !row.modules.every(isModuleHidden));
}
