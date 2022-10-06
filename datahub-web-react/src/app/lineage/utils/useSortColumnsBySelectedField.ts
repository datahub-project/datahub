import { useContext, useEffect } from 'react';
import usePrevious from '../../shared/usePrevious';
import { NUM_COLUMNS_PER_PAGE } from '../constants';
import { FetchedEntity } from '../types';
import { getHighlightedColumnsForNode, sortColumnsByDefault, sortRelatedLineageColumns } from './columnLineageUtils';
import { LineageExplorerContext } from './LineageExplorerContext';

export default function useSortColumnsBySelectedField(fetchedEntities: { [x: string]: FetchedEntity }) {
    const { highlightedEdges, selectedField, columnsByUrn, setColumnsByUrn } = useContext(LineageExplorerContext);
    const previousSelectedField = usePrevious(selectedField);

    useEffect(() => {
        let updatedColumnsByUrn = { ...columnsByUrn };

        if (selectedField && previousSelectedField !== selectedField) {
            Object.entries(columnsByUrn).forEach(([urn, columns]) => {
                if (selectedField.urn !== urn && columns.length >= NUM_COLUMNS_PER_PAGE) {
                    const highlightedColumnsForNode = getHighlightedColumnsForNode(highlightedEdges, columns, urn);

                    if (highlightedColumnsForNode.length > 0) {
                        updatedColumnsByUrn = sortRelatedLineageColumns(
                            highlightedColumnsForNode,
                            columns,
                            urn,
                            updatedColumnsByUrn,
                        );
                    }
                }
            });
            setColumnsByUrn(updatedColumnsByUrn);
        } else if (!selectedField && previousSelectedField !== selectedField) {
            Object.entries(columnsByUrn).forEach(([urn, columns]) => {
                const fetchedEntity = fetchedEntities[urn];
                if (fetchedEntity && fetchedEntity.schemaMetadata) {
                    updatedColumnsByUrn = sortColumnsByDefault(
                        updatedColumnsByUrn,
                        columns,
                        fetchedEntity.schemaMetadata.fields,
                        urn,
                    );
                }
            });
            setColumnsByUrn(updatedColumnsByUrn);
        }
    }, [selectedField, previousSelectedField, highlightedEdges, columnsByUrn, fetchedEntities, setColumnsByUrn]);
}
