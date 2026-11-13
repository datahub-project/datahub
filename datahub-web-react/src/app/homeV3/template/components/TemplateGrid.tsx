import React, { memo, useMemo } from 'react';

import NewRowDropZone from '@app/homeV3/template/components/NewRowDropZone';
import TemplateRow from '@app/homeV3/templateRow/TemplateRow';
import { useIsModuleHidden } from '@app/homeV3/templateRow/hooks/useIsModuleHidden';
import { WrappedRow } from '@app/homeV3/templateRow/types';
import { getVisibleRows } from '@app/homeV3/templateRow/utils';

interface Props {
    wrappedRows: WrappedRow[];
}

function TemplateGrid({ wrappedRows }: Props) {
    const isModuleHidden = useIsModuleHidden();

    // Memoize the template rows with drop zones between them
    const templateRowsWithDropZones = useMemo(() => {
        const result: React.ReactElement[] = [];

        getVisibleRows(wrappedRows, isModuleHidden).forEach((row, i) => {
            const { rowIndex } = row;
            // Add drop zone before the first row (for inserting at beginning)
            if (i === 0) {
                result.push(<NewRowDropZone key="drop-zone-before-0" rowIndex={0} insertNewRow />);
            }

            // Add the actual row
            const rowKey = `templateRow-${rowIndex}`;
            result.push(<TemplateRow key={rowKey} row={row} rowIndex={rowIndex} />);

            // Add drop zone after each row (for inserting between/after rows)
            const finalDropKey = `drop-zone-after-${rowIndex}`;
            result.push(<NewRowDropZone key={finalDropKey} rowIndex={rowIndex + 1} insertNewRow />);
        });

        return result;
    }, [wrappedRows, isModuleHidden]);

    return <>{templateRowsWithDropZones}</>;
}

export default memo(TemplateGrid);
