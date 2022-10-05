import { useContext, useEffect } from 'react';
import usePrevious from '../../shared/usePrevious';
import { NUM_COLUMNS_PER_PAGE } from '../constants';
import { NodeData } from '../types';
import { getHighlightedColumnsForNode, sortColumnsByDefault, sortRelatedLineageColumns } from './columnLineageUtils';
import { LineageExplorerContext } from './LineageExplorerContext';

export default function useSortColumnsBySelectedField(
    node: { x: number; y: number; data: Omit<NodeData, 'children'> },
    setPageIndex: (index: number) => void,
    setHaveFieldsBeenUpdated: (haveBeenUpdated: boolean) => void,
) {
    const { highlightedEdges, selectedField, columnsByUrn, setColumnsByUrn } = useContext(LineageExplorerContext);
    const fields = columnsByUrn[node.data.urn || ''] || node.data.schemaMetadata?.fields;
    const previousSelectedField = usePrevious(selectedField);

    useEffect(() => {
        if (
            selectedField &&
            previousSelectedField !== selectedField &&
            selectedField.urn !== node.data.urn &&
            fields.length >= NUM_COLUMNS_PER_PAGE
        ) {
            const highlightedColumnsForNode = getHighlightedColumnsForNode(
                highlightedEdges,
                fields,
                node.data.urn || '',
            );

            if (highlightedColumnsForNode.length > 0) {
                setColumnsByUrn((colsByUrn) =>
                    sortRelatedLineageColumns(highlightedColumnsForNode, fields, node.data.urn || '', colsByUrn),
                );
                setPageIndex(0);
                setHaveFieldsBeenUpdated(true);
            }
        } else if (!selectedField && previousSelectedField !== selectedField) {
            setColumnsByUrn((colsByUrn) =>
                sortColumnsByDefault(colsByUrn, fields, node.data.schemaMetadata?.fields || [], node.data.urn || ''),
            );
            setHaveFieldsBeenUpdated(true);
        }
    }, [
        selectedField,
        previousSelectedField,
        highlightedEdges,
        node.data.urn,
        fields,
        setColumnsByUrn,
        setPageIndex,
        setHaveFieldsBeenUpdated,
        node.data.schemaMetadata?.fields,
    ]);
}
