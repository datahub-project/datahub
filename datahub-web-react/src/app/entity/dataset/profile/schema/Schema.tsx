import React, { useCallback, useState, useEffect } from 'react';
import { Typography } from 'antd';
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
import {
    convertEditableSchemaMetadataForUpdate,
    diffJson,
    fieldPathSortAndParse,
    ExtendedSchemaFields,
    getRawSchema,
} from '../../../shared/utils';
import { convertTagsForUpdate } from '../../../../shared/tags/utils/convertTagsForUpdate';
import SchemaTable from './SchemaTable';
import SchemaVersionSummary from './components/SchemaVersionSummary';
import SchemaHeader from './components/SchemaHeader';
import analytics, { EventType, EntityActionType } from '../../../../analytics';

const SchemaContainer = styled.div`
    margin-bottom: 100px;
`;

export type Props = {
    urn: string;
    usageStats?: UsageQueryResult | null;
    schema?: SchemaMetadata | Schema | null;
    pastSchemaMetadata?: SchemaMetadata | null;
    editableSchemaMetadata?: EditableSchemaMetadata | null;
    updateEditableSchema: (
        update: EditableSchemaMetadataUpdate,
    ) => Promise<FetchResult<UpdateDatasetMutation, Record<string, any>, Record<string, any>>>;
};

export default function SchemaView({
    urn,
    schema,
    pastSchemaMetadata,
    editableSchemaMetadata,
    updateEditableSchema,
    usageStats,
}: Props) {
    const totalVersions = pastSchemaMetadata?.aspectVersion || 0;
    const [showRaw, setShowRaw] = useState(false);
    const [diffSummary, setDiffSummary] = useState({ added: 0, removed: 0, updated: 0, schemaRawUpdated: false });
    const [editMode, setEditMode] = useState(true);
    const [currentVersion, setCurrentVersion] = useState(totalVersions);
    const [rows, setRows] = useState<Array<ExtendedSchemaFields>>([]);
    const [schemaRawDiff, setSchemaRawDiff] = useState<string>('');
    const [getSchemaVersions, { loading, error, data: schemaVersions }] = useGetDatasetSchemaVersionsLazyQuery({
        fetchPolicy: 'no-cache',
    });

    const updateDiff = useCallback(
        (currentSchema?: SchemaMetadata | Schema | null, pastSchema?: SchemaMetadata | null) => {
            const { fields, added, removed, updated } = fieldPathSortAndParse(
                currentSchema?.fields,
                pastSchema?.fields,
                editMode,
            );
            const currentSchemaRaw =
                currentSchema?.platformSchema?.__typename === 'TableSchema'
                    ? getRawSchema(currentSchema.platformSchema.schema)
                    : '';
            if (!editMode) {
                const pastSchemaRaw =
                    pastSchema?.platformSchema?.__typename === 'TableSchema'
                        ? getRawSchema(pastSchema.platformSchema.schema)
                        : '';
                setSchemaRawDiff(diffJson(pastSchemaRaw, currentSchemaRaw));
                setDiffSummary({ added, removed, updated, schemaRawUpdated: currentSchemaRaw === pastSchemaRaw });
            } else {
                setSchemaRawDiff(currentSchemaRaw);
            }
            setRows(fields);
        },
        [editMode],
    );

    useEffect(() => {
        if (!loading && !error && schemaVersions) {
            updateDiff(schemaVersions.dataset?.schemaMetadata, schemaVersions.dataset?.pastSchemaMetadata);
        }
    }, [schemaVersions, loading, error, updateDiff]);

    useEffect(() => {
        updateDiff(schema, pastSchemaMetadata);
    }, [schema, pastSchemaMetadata, updateDiff]);

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
            {loading && <Message type="loading" content="Loading..." style={{ marginTop: '30%' }} />}
            <SchemaHeader
                currentVersion={currentVersion}
                setCurrentVersion={setCurrentVersion}
                totalVersions={totalVersions}
                updateDiff={() => updateDiff(schema, pastSchemaMetadata)}
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
                <Typography.Text data-testid="schema-raw-view">
                    <pre>
                        <code>{schemaRawDiff}</code>
                    </pre>
                </Typography.Text>
            ) : (
                rows &&
                rows.length > 0 && (
                    <>
                        {!editMode && (
                            <SchemaVersionSummary diffSummary={diffSummary} currentVersion={currentVersion} />
                        )}
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
