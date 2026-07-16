import { useContext, useEffect } from 'react';

import { NUM_COLUMNS_PER_PAGE } from '@app/lineage/constants';
import { LineageExplorerContext } from '@app/lineage/utils/LineageExplorerContext';
import { getHighlightedColumnsForNode } from '@app/lineage/utils/columnLineageUtils';
import usePrevious from '@app/shared/usePrevious';

import { SchemaField } from '@types';

export function useResetPageIndexAfterSelect(
    nodeUrn: string,
    fields: SchemaField[],
    setPageIndex: (pageIndex: number) => void,
) {
    const { selectedField, highlightedEdges } = useContext(LineageExplorerContext);
    const previousSelectedField = usePrevious(selectedField);

    useEffect(() => {
        // all of this logic is to determine if we've reordered this node's fields when clicking a selected field somewhere
        if (
            selectedField &&
            previousSelectedField !== selectedField &&
            selectedField.urn !== nodeUrn &&
            fields.length >= NUM_COLUMNS_PER_PAGE
        ) {
            const highlightedColumnsForNode = getHighlightedColumnsForNode(highlightedEdges, fields, nodeUrn || '');

            if (highlightedColumnsForNode.length > 0) {
                // at this point we know this node's columns have been reordered, set them on first page
                setPageIndex(0);
            }
        }
    }, [selectedField, previousSelectedField, nodeUrn, fields, highlightedEdges, setPageIndex]);
}
