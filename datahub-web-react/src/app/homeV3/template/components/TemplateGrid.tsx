import React, { memo, useMemo } from 'react';

import { ModulesAvailableToAdd } from '@app/homeV3/modules/types';
import TemplateRow from '@app/homeV3/templateRow/TemplateRow';
import { WrappedRow } from '@app/homeV3/templateRow/types';

import NewRowDropZone from './NewRowDropZone';

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
                result.push(<NewRowDropZone key={`drop-zone-before-${i}`} rowIndex={i} insertNewRow={true} />);
            }

            // Add the actual row
            result.push(
                <TemplateRow
                    key={`templateRow-${i}`}
                    row={row}
                    rowIndex={i}
                    modulesAvailableToAdd={modulesAvailableToAdd}
                />,
            );

            // Add drop zone after each row (for inserting between/after rows)
            result.push(<NewRowDropZone key={`drop-zone-after-${i}`} rowIndex={i + 1} insertNewRow={true} />);
        });

        return result;
    }, [wrappedRows, modulesAvailableToAdd]);

    return <>{templateRowsWithDropZones}</>;
}

export default memo(TemplateGrid); 