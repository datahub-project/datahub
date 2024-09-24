import { useContext, useEffect } from 'react';
import { SchemaField } from '../../../types.generated';
import usePrevious from '../../shared/usePrevious';
import { NUM_COLUMNS_PER_PAGE } from '../constants';
import { FetchedEntity } from '../types';
import {
    convertFieldsToV1FieldPath,
    convertInputFieldsToSchemaFields,
    getHighlightedColumnsForNode,
    sortColumnsByDefault,
    sortRelatedLineageColumns,
} from './columnLineageUtils';
import { LineageExplorerContext } from './LineageExplorerContext';

export default function useSortColumnsBySelectedField(fetchedEntities: Map<string, FetchedEntity>) {
    const { highlightedEdges, selectedField, columnsByUrn, setColumnsByUrn } = useContext(LineageExplorerContext);
    const previousSelectedField = usePrevious(selectedField);

    useEffect(() => {
        let updatedColumnsByUrn = { ...columnsByUrn };

        if (selectedField && previousSelectedField !== selectedField) {
            Object.entries(columnsByUrn).forEach(([urn, columns]) => {
                if (selectedField.urn !== urn && columns.length > NUM_COLUMNS_PER_PAGE) {
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
                const fetchedEntity = fetchedEntities.get(urn);
                if (fetchedEntity?.schemaMetadata) {
                    updatedColumnsByUrn = sortColumnsByDefault(
                        updatedColumnsByUrn,
                        columns,
                        convertFieldsToV1FieldPath(fetchedEntity.schemaMetadata.fields),
                        urn,
                    );
                } else if (fetchedEntity?.inputFields) {
                    updatedColumnsByUrn = sortColumnsByDefault(
                        updatedColumnsByUrn,
                        columns,
                        convertFieldsToV1FieldPath(
                            convertInputFieldsToSchemaFields(fetchedEntity.inputFields) as SchemaField[],
                        ),
                        urn,
                    );
                }
            });
            setColumnsByUrn(updatedColumnsByUrn);
        }
    }, [selectedField, previousSelectedField, highlightedEdges, columnsByUrn, fetchedEntities, setColumnsByUrn]);
}
