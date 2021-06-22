import React, { useState, useEffect } from 'react';
import { Button, Pagination, Typography } from 'antd';
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
} from '../../../../../types.generated';
import { convertEditableSchemaMeta, fieldPathSortAndParse, ExtendedSchemaFields } from '../../../shared/utils';
import { convertTagsForUpdate } from '../../../../shared/tags/utils/convertTagsForUpdate';
import SchemaTable from './SchemaTable';
import analytics, { EventType, EntityActionType } from '../../../../analytics';

const SchemaContainer = styled.div`
    margin-bottom: 100px;
`;
const ViewRawButtonContainer = styled.div`
    display: flex;
    justify-content: flex-end;
    padding-bottom: 16px;
`;

const ShowVersionButton = styled(Button)`
    margin-right: 15px;
`;
const PaginationContainer = styled(Pagination)`
    padding-top: 6px;
    margin-right: 10px;
`;

export function convertEditableSchemaMetadataForUpdate(
    editableSchemaMetadata: EditableSchemaMetadata | null | undefined,
): EditableSchemaMetadataUpdate {
    return {
        editableSchemaFieldInfo:
            editableSchemaMetadata?.editableSchemaFieldInfo.map((editableSchemaFieldInfo) => ({
                fieldPath: editableSchemaFieldInfo?.fieldPath,
                description: editableSchemaFieldInfo?.description,
                globalTags: { tags: convertTagsForUpdate(editableSchemaFieldInfo?.globalTags?.tags || []) },
            })) || [],
    };
}

export type Props = {
    urn: string;
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
}: Props) {
    const [showRaw, setShowRaw] = useState(false);
    const [showVersions, setShowVersions] = useState(false);
    const [rows, setRows] = useState<Array<ExtendedSchemaFields>>([]);
    const [getSchemaVersions, { loading, error, data: schemaVersions }] = useGetDatasetSchemaVersionsLazyQuery({
        fetchPolicy: 'no-cache',
    });
    const hasEditableSchemaMeta =
        editableSchemaMetadata &&
        editableSchemaMetadata.editableSchemaFieldInfo &&
        editableSchemaMetadata.editableSchemaFieldInfo.length > 0;
    const totalVersions = hasEditableSchemaMeta
        ? (pastSchemaMetadata?.aspectVersion || 0) + 1
        : pastSchemaMetadata?.aspectVersion || 0;

    useEffect(() => {
        if (!loading && !error && schemaVersions) {
            setRows(
                fieldPathSortAndParse(
                    schemaVersions.dataset?.schemaMetadata?.fields,
                    schemaVersions.dataset?.pastSchemaMetadata?.fields,
                    !showVersions,
                ),
            );
        }
    }, [schemaVersions, loading, error, showVersions]);

    useEffect(() => {
        if (hasEditableSchemaMeta && (schema?.fields || editableSchemaMetadata?.editableSchemaFieldInfo)) {
            setRows(
                fieldPathSortAndParse(
                    convertEditableSchemaMeta(editableSchemaMetadata?.editableSchemaFieldInfo, schema?.fields),
                    schema?.fields,
                    !showVersions,
                ),
            );
        } else if (schema?.fields || pastSchemaMetadata?.fields) {
            setRows(fieldPathSortAndParse(schema?.fields, pastSchemaMetadata?.fields, !showVersions));
        }
    }, [
        schema?.fields,
        pastSchemaMetadata?.fields,
        showVersions,
        hasEditableSchemaMeta,
        editableSchemaMetadata,
        editableSchemaMetadata?.editableSchemaFieldInfo,
    ]);

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

    const getRawSchema = (schemaValue) => {
        try {
            return JSON.stringify(JSON.parse(schemaValue), null, 2);
        } catch (e) {
            return schemaValue;
        }
    };

    const onVersionChange = (page: number) => {
        if (page === 1) {
            if (hasEditableSchemaMeta && (schema?.fields || editableSchemaMetadata?.editableSchemaFieldInfo)) {
                setRows(
                    fieldPathSortAndParse(
                        convertEditableSchemaMeta(editableSchemaMetadata?.editableSchemaFieldInfo, schema?.fields),
                        schema?.fields,
                        !showVersions,
                    ),
                );
            } else if (schema?.fields || pastSchemaMetadata?.fields) {
                setRows(fieldPathSortAndParse(schema?.fields, pastSchemaMetadata?.fields, !showVersions));
            }
            return;
        }
        const version1 = hasEditableSchemaMeta ? -page + 2 : -page + 1;
        getSchemaVersions({
            variables: {
                urn,
                version1,
                version2: version1 - 1,
            },
        });
    };

    return (
        <SchemaContainer>
            {loading && <Message type="loading" content="Loading..." style={{ marginTop: '30%' }} />}
            <ViewRawButtonContainer>
                {totalVersions > 0 &&
                    (!showVersions ? (
                        <ShowVersionButton onClick={() => setShowVersions(true)}>Version History</ShowVersionButton>
                    ) : (
                        <>
                            <Button type="text" onClick={() => setShowVersions(false)}>
                                Version
                            </Button>
                            <PaginationContainer
                                simple
                                size="default"
                                defaultCurrent={1}
                                defaultPageSize={1}
                                total={totalVersions}
                                onChange={onVersionChange}
                            />
                        </>
                    ))}
                {schema?.platformSchema?.__typename === 'TableSchema' && schema?.platformSchema?.schema?.length > 0 && (
                    <Button onClick={() => setShowRaw(!showRaw)}>{showRaw ? 'Tabular' : 'Raw'}</Button>
                )}
            </ViewRawButtonContainer>
            {showRaw ? (
                <Typography.Text data-testid="schema-raw-view">
                    <pre>
                        <code>
                            {schema?.platformSchema?.__typename === 'TableSchema' &&
                                getRawSchema(schema.platformSchema.schema)}
                        </code>
                    </pre>
                </Typography.Text>
            ) : (
                rows.length > 0 && (
                    <SchemaTable
                        rows={rows}
                        editMode={!showVersions}
                        onUpdateDescription={onUpdateDescription}
                        onUpdateTags={onUpdateTags}
                        editableSchemaMetadata={editableSchemaMetadata}
                    />
                )
            )}
        </SchemaContainer>
    );
}
