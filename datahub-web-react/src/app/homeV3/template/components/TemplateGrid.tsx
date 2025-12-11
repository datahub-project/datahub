/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { memo, useMemo } from 'react';

import NewRowDropZone from '@app/homeV3/template/components/NewRowDropZone';
import TemplateRow from '@app/homeV3/templateRow/TemplateRow';
import { WrappedRow } from '@app/homeV3/templateRow/types';

interface Props {
    wrappedRows: WrappedRow[];
}

function TemplateGrid({ wrappedRows }: Props) {
    // Memoize the template rows with drop zones between them
    const templateRowsWithDropZones = useMemo(() => {
        const result: React.ReactElement[] = [];

        wrappedRows.forEach((row, i) => {
            // Add drop zone before the first row (for inserting at beginning)
            if (i === 0) {
                const initialDropKey = `drop-zone-before-${i}`;
                result.push(<NewRowDropZone key={initialDropKey} rowIndex={i} insertNewRow />);
            }

            // Add the actual row
            const rowKey = `templateRow-${i}`;
            result.push(<TemplateRow key={rowKey} row={row} rowIndex={i} />);

            // Add drop zone after each row (for inserting between/after rows)
            const finalDropKey = `drop-zone-after-${i}`;
            result.push(<NewRowDropZone key={finalDropKey} rowIndex={i + 1} insertNewRow />);
        });

        return result;
    }, [wrappedRows]);

    return <>{templateRowsWithDropZones}</>;
}

export default memo(TemplateGrid);
