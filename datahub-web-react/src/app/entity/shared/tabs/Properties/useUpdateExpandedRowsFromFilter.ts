import { useEffect } from 'react';
import { isEqual } from 'lodash';
import usePrevious from '../../../../shared/usePrevious';

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
