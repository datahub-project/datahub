import React, { useMemo, useState, useEffect } from 'react';
import styled from 'styled-components';
import { FetchResult } from '@apollo/client';
import { Message } from '../../../../shared/Message';
import { useGetDatasetSchemaVersionsLazyQuery, UpdateDatasetMutation } from '../../../../../graphql/dataset.generated';
import {
    Schema,
    SchemaMetadata,
    EditableSchemaMetadata,
    EditableSchemaMetadataUpdate,
    GlobalTagsUpdate,
    EditableSchemaFieldInfo,
    EditableSchemaFieldInfoUpdate,
    EntityType,
    UsageQueryResult,
} from '../../../../../types.generated';
import { convertTagsForUpdate } from '../../../../shared/tags/utils/convertTagsForUpdate';
import SchemaTable from './SchemaTable';
import SchemaHeader from './components/SchemaHeader';
import SchemaRawView from './components/SchemaRawView';
import SchemaVersionSummary from './components/SchemaVersionSummary';
import analytics, { EventType, EntityActionType } from '../../../../analytics';
import { KEY_SCHEMA_PREFIX } from './utils/constants';
import {
    convertEditableSchemaMetadataForUpdate,
    getDiffSummary,
    groupByFieldPath,
    pathMatchesNewPath,
} from './utils/utils';
import { ExtendedSchemaFields } from './utils/types';

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

    const hasKeySchema = useMemo(
        () => (schema?.fields?.findIndex((field) => field.fieldPath.indexOf(KEY_SCHEMA_PREFIX) > -1) || -1) !== -1,
        [schema],
    );

    const [showKeySchema, setShowKeySchema] = useState(false);

    const { rows, diffSummary } = useMemo(() => {
        if (editMode) {
            return { rows: groupByFieldPath(schemaDiff.current?.fields, { showKeySchema }), diffSummary: null };
        }
        const rowsAndDiffSummary = getDiffSummary(schemaDiff.current?.fields, schemaDiff.previous?.fields, {
            showKeySchema,
        });
        return {
            ...rowsAndDiffSummary,
            rows: groupByFieldPath(rowsAndDiffSummary.rows, { showKeySchema }),
        };
    }, [schemaDiff, editMode, showKeySchema]);

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

        if (
            existingMetadataAsUpdate.editableSchemaFieldInfo.some((field) =>
                pathMatchesNewPath(field.fieldPath, record?.fieldPath),
            )
        ) {
            // if we already have a record for this field, update the record
            existingMetadataAsUpdate = {
                editableSchemaFieldInfo: existingMetadataAsUpdate.editableSchemaFieldInfo.map((fieldUpdate) => {
                    if (pathMatchesNewPath(fieldUpdate.fieldPath, record?.fieldPath)) {
                        return newFieldInfo;
                    }

                    // migrate any old fields that exist
                    const upgradedFieldPath = schema?.fields.find((field) =>
                        pathMatchesNewPath(fieldUpdate.fieldPath, field.fieldPath),
                    )?.fieldPath;

                    if (upgradedFieldPath) {
                        // eslint-disable-next-line no-param-reassign
                        fieldUpdate.fieldPath = upgradedFieldPath;
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
                hasRaw={
                    schema?.platformSchema?.__typename === 'TableSchema' && schema?.platformSchema?.schema?.length > 0
                }
                hasKeySchema={hasKeySchema}
                showKeySchema={showKeySchema}
                setShowKeySchema={setShowKeySchema}
            />
            {showRaw ? (
                <SchemaRawView schemaDiff={schemaDiff} editMode={editMode} showKeySchema={showKeySchema} />
            ) : (
                rows &&
                rows.length > 0 && (
                    <>
                        {!editMode && diffSummary ? <SchemaVersionSummary diffSummary={diffSummary} /> : null}
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
