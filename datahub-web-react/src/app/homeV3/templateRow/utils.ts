/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
