import React, { memo, useMemo } from 'react';

import { ModulesAvailableToAdd } from '@app/homeV3/modules/types';
import NewRowDropZone from '@app/homeV3/template/components/NewRowDropZone';
import TemplateRow from '@app/homeV3/templateRow/TemplateRow';
import { WrappedRow } from '@app/homeV3/templateRow/types';

// import NewRowDropZone from './NewRowDropZone';

interface Props {
    wrappedRows: WrappedRow[];
    modulesAvailableToAdd: ModulesAvailableToAdd;
}

function TemplateGrid({ wrappedRows, modulesAvailableToAdd }: Props) {
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
            result.push(
                <TemplateRow key={rowKey} row={row} rowIndex={i} modulesAvailableToAdd={modulesAvailableToAdd} />,
            );

            // Add drop zone after each row (for inserting between/after rows)
            const finalDropKey = `drop-zone-after-${i}`;
            result.push(<NewRowDropZone key={finalDropKey} rowIndex={i + 1} insertNewRow />);
        });

        return result;
    }, [wrappedRows, modulesAvailableToAdd]);

    return <>{templateRowsWithDropZones}</>;
}

export default memo(TemplateGrid);
