import React, { useMemo, useState, useEffect } from 'react';
import styled from 'styled-components';
import { FetchResult } from '@apollo/client';
import { Message } from '../../../../shared/Message';
import { useGetDatasetSchemaVersionsLazyQuery, UpdateDatasetMutation } from '../../../../../graphql/dataset.generated';
import {
    Schema,
    SchemaField,
    SchemaMetadata,
    EditableSchemaMetadata,
    EditableSchemaMetadataUpdate,
    GlobalTagsUpdate,
    EditableSchemaFieldInfo,
    EditableSchemaFieldInfoUpdate,
    EntityType,
    UsageQueryResult,
} from '../../../../../types.generated';
import {
    convertEditableSchemaMetadataForUpdate,
    ExtendedSchemaFields,
    sortByFieldPathAndGrouping,
} from '../../../shared/utils';
import { convertTagsForUpdate } from '../../../../shared/tags/utils/convertTagsForUpdate';
import SchemaTable from './SchemaTable';
import SchemaHeader from './components/SchemaHeader';
import SchemaRawView from './components/SchemaRawView';
import SchemaVersionSummary, { SchemaDiffSummary } from './components/SchemaVersionSummary';
import analytics, { EventType, EntityActionType } from '../../../../analytics';

const SchemaContainer = styled.div`
    margin-bottom: 100px;
`;

export type Props = {
    urn: string;
    usageStats?: UsageQueryResult | null;
    schema?: SchemaMetadata | Schema | null;
    previousSchemaMetadata?: SchemaMetadata | null;
    editableSchemaMetadata?: EditableSchemaMetadata | null;
    updateEditableSchema: (
        update: EditableSchemaMetadataUpdate,
    ) => Promise<FetchResult<UpdateDatasetMutation, Record<string, any>, Record<string, any>>>;
};

// Get diff summary between two versions and prepare to visualize description diff changes
const getDiffSummary = (
    currentVersionRows?: Array<SchemaField>,
    previousVersionRows?: Array<SchemaField>,
): { rows: Array<ExtendedSchemaFields>; diffSummary: SchemaDiffSummary } => {
    let rows = [...(currentVersionRows || [])] as Array<ExtendedSchemaFields>;
    const diffSummary: SchemaDiffSummary = {
        added: 0,
        removed: 0,
        updated: 0,
    };

    if (previousVersionRows && previousVersionRows.length > 0) {
        const previousRows = [...previousVersionRows] as Array<ExtendedSchemaFields>;
        rows.forEach((field, rowIndex) => {
            const relevantPastFieldIndex = previousRows.findIndex(
                (pf) => pf.type === rows[rowIndex].type && pf.fieldPath === rows[rowIndex].fieldPath,
            );
            if (relevantPastFieldIndex > -1) {
                if (previousRows[relevantPastFieldIndex].description !== rows[rowIndex].description) {
                    rows[rowIndex] = {
                        ...rows[rowIndex],
                        previousDescription: previousRows[relevantPastFieldIndex].description,
                    };
                    diffSummary.updated++; // Increase updated row number in diff summary
                }
                previousRows.splice(relevantPastFieldIndex, 1);
            } else {
                rows[rowIndex] = { ...rows[rowIndex], isNewRow: true };
                diffSummary.added++; // Increase added row number in diff summary
            }
        });
        rows = [...rows, ...previousRows.map((pf) => ({ ...pf, isDeletedRow: true }))];
        diffSummary.removed = previousRows.length; // removed row number in diff summary
    }

    return { rows, diffSummary };
};

export default function SchemaView({
    urn,
    schema,
    previousSchemaMetadata,
    editableSchemaMetadata,
    updateEditableSchema,
    usageStats,
}: Props) {
    const maxVersion = previousSchemaMetadata?.aspectVersion || 0;
    const [showRaw, setShowRaw] = useState(false);
    const [editMode, setEditMode] = useState(true);
    const [schemaDiff, setSchemaDiff] = useState<{
        current?: SchemaMetadata | Schema | null;
        previous?: SchemaMetadata | null;
    }>({ current: schema, previous: previousSchemaMetadata });
    const [getSchemaVersions, { loading, error, data: schemaVersions }] = useGetDatasetSchemaVersionsLazyQuery({
        fetchPolicy: 'no-cache',
    });

    const { rows, diffSummary } = useMemo(() => {
        if (editMode) {
            return { rows: sortByFieldPathAndGrouping(schemaDiff.current?.fields), diffSummary: null };
        }
        const rowsAndDiffSummary = getDiffSummary(schemaDiff.current?.fields, schemaDiff.previous?.fields);
        return {
            ...rowsAndDiffSummary,
            rows: sortByFieldPathAndGrouping(rowsAndDiffSummary.rows),
        };
    }, [schemaDiff, editMode]);

    useEffect(() => {
        if (!loading && !error && schemaVersions) {
            setSchemaDiff({
                current: schemaVersions.dataset?.schemaMetadata,
                previous: schemaVersions.dataset?.previousSchemaMetadata,
            });
        }
    }, [schemaVersions, loading, error]);

    const updateSchema = (newFieldInfo: EditableSchemaFieldInfoUpdate, record?: EditableSchemaFieldInfo) => {
        let existingMetadataAsUpdate = convertEditableSchemaMetadataForUpdate(editableSchemaMetadata);

        if (existingMetadataAsUpdate.editableSchemaFieldInfo.some((field) => field.fieldPath === record?.fieldPath)) {
            // if we already have a record for this field, update the record
            existingMetadataAsUpdate = {
                editableSchemaFieldInfo: existingMetadataAsUpdate.editableSchemaFieldInfo.map((fieldUpdate) => {
                    if (fieldUpdate.fieldPath === record?.fieldPath) {
                        return newFieldInfo;
                    }
                    return fieldUpdate;
                }),
            };
        } else {
            // otherwise add a new record
            existingMetadataAsUpdate.editableSchemaFieldInfo.push(newFieldInfo);
        }
        return updateEditableSchema(existingMetadataAsUpdate);
    };

    const onUpdateTags = (update: GlobalTagsUpdate, record?: EditableSchemaFieldInfo) => {
        if (!record) return Promise.resolve();
        analytics.event({
            type: EventType.EntityActionEvent,
            actionType: EntityActionType.UpdateSchemaTags,
            entityType: EntityType.Dataset,
            entityUrn: urn,
        });
        const newFieldInfo: EditableSchemaFieldInfoUpdate = {
            fieldPath: record?.fieldPath,
            description: record?.description,
            globalTags: update,
        };
        return updateSchema(newFieldInfo, record);
    };

    const onUpdateDescription = (
        updatedDescription: string,
        record?: EditableSchemaFieldInfo | ExtendedSchemaFields,
    ) => {
        if (!record) return Promise.resolve();
        analytics.event({
            type: EventType.EntityActionEvent,
            actionType: EntityActionType.UpdateSchemaDescription,
            entityType: EntityType.Dataset,
            entityUrn: urn,
        });
        const newFieldInfo: EditableSchemaFieldInfoUpdate = {
            fieldPath: record?.fieldPath,
            description: updatedDescription,
            globalTags: { tags: convertTagsForUpdate(record?.globalTags?.tags || []) },
        };
        return updateSchema(newFieldInfo, record as EditableSchemaFieldInfo);
    };

    const fetchVersions = (version1: number, version2: number) => {
        getSchemaVersions({
            variables: {
                urn,
                version1,
                version2,
            },
        });
    };

    return (
        <SchemaContainer>
            {loading && <Message type="loading" content="" style={{ marginTop: '35%' }} />}
            <SchemaHeader
                maxVersion={maxVersion}
                fetchVersions={fetchVersions}
                editMode={editMode}
                setEditMode={setEditMode}
                showRaw={showRaw}
                setShowRaw={setShowRaw}
                hasRow={
                    schema?.platformSchema?.__typename === 'TableSchema' && schema?.platformSchema?.schema?.length > 0
                }
            />
            {showRaw ? (
                <SchemaRawView schemaDiff={schemaDiff} editMode={editMode} />
            ) : (
                rows &&
                rows.length > 0 && (
                    <>
                        {!editMode && diffSummary && <SchemaVersionSummary diffSummary={diffSummary} />}
                        <SchemaTable
                            rows={rows}
                            editMode={editMode}
                            onUpdateDescription={onUpdateDescription}
                            onUpdateTags={onUpdateTags}
                            editableSchemaMetadata={editableSchemaMetadata}
                            usageStats={usageStats}
                        />
                    </>
                )
            )}
        </SchemaContainer>
    );
}
