/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { isEqual } from 'lodash';
import { useEffect } from 'react';

import usePrevious from '@app/shared/usePrevious';

interface Props {
    expandedRowsFromFilter: Set<string>;
    setExpandedRows: React.Dispatch<React.SetStateAction<Set<string>>>;
}

export default function useUpdateExpandedRowsFromFilter({ expandedRowsFromFilter, setExpandedRows }: Props) {
    const previousExpandedRowsFromFilter = usePrevious(expandedRowsFromFilter);

    useEffect(() => {
        if (!isEqual(expandedRowsFromFilter, previousExpandedRowsFromFilter)) {
            setExpandedRows((previousRows) => {
                const finalRowsSet = new Set();
                expandedRowsFromFilter.forEach((row) => finalRowsSet.add(row));
                previousRows.forEach((row) => finalRowsSet.add(row));
                return finalRowsSet as Set<string>;
            });
        }
    }, [expandedRowsFromFilter, previousExpandedRowsFromFilter, setExpandedRows]);
}
