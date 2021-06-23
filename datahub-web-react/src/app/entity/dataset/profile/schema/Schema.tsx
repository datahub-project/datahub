import React, { useCallback, useState, useEffect } from 'react';
import { Button, InputNumber, Typography } from 'antd';
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
} from '../../../../../types.generated';
import { fieldPathSortAndParse, ExtendedSchemaFields } from '../../../shared/utils';
import { convertTagsForUpdate } from '../../../../shared/tags/utils/convertTagsForUpdate';
import SchemaTable from './SchemaTable';
import analytics, { EventType, EntityActionType } from '../../../../analytics';

const SchemaContainer = styled.div`
    margin-bottom: 100px;
`;
const ViewRawButtonContainer = styled.div<{ padding?: string }>`
    display: flex;
    justify-content: flex-end;
    ${(props) => (props.padding ? 'padding-bottom: 16px;' : '')}
`;

const ShowVersionButton = styled(Button)`
    margin-right: 15px;
`;
const InputNumberContainer = styled(InputNumber)`
    margin: 0 0 0 3px;
    width: 30px;
    & input {
        padding: 0;
        font-weight: bold;
        padding-top: 2px;
    }
    & .ant-input-number-handler-wrap {
        background: transparent;
    }
`;
const VersionText = styled(Typography.Text)`
    line-height: 32px;
`;
const VersionRightText = styled(Typography.Text)`
    margin-left: 3px;
    margin-right: 20px;
    line-height: 32px;
`;
const SummaryContainer = styled.ul`
    margin-bottom: 16px;
    float: right;
    padding-right: 60px;
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
    const [diffSummary, setDiffSummary] = useState({ added: 0, removed: 0, updated: 0 });
    const [showVersions, setShowVersions] = useState(false);
    const [currentVersion, setCurrentVersion] = useState(0);
    const [rows, setRows] = useState<Array<ExtendedSchemaFields>>([]);
    const [getSchemaVersions, { loading, error, data: schemaVersions }] = useGetDatasetSchemaVersionsLazyQuery({
        fetchPolicy: 'no-cache',
    });
    const totalVersions = pastSchemaMetadata?.aspectVersion || 0;
    const updateDiff = useCallback(
        (newFields?: Array<SchemaField>, oldFields?: Array<SchemaField>) => {
            const { fields, added, removed, updated } = fieldPathSortAndParse(newFields, oldFields, !showVersions);
            setRows(fields);
            setDiffSummary({ added, removed, updated });
        },
        [showVersions],
    );

    useEffect(() => {
        if (!loading && !error && schemaVersions) {
            updateDiff(
                schemaVersions.dataset?.schemaMetadata?.fields,
                schemaVersions.dataset?.pastSchemaMetadata?.fields,
            );
        }
    }, [schemaVersions, loading, error, updateDiff]);

    useEffect(() => {
        updateDiff(schema?.fields, pastSchemaMetadata?.fields);
    }, [schema?.fields, pastSchemaMetadata?.fields, updateDiff]);

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

    const onVersionChange = (version) => {
        if (version <= -totalVersions) {
            setCurrentVersion(-totalVersions + 1);
            return;
        }
        if (version > 0) {
            setCurrentVersion(0);
            return;
        }
        if (version === null) {
            return;
        }
        setCurrentVersion(version);
        if (version === 0) {
            updateDiff(schema?.fields, pastSchemaMetadata?.fields);
            return;
        }
        getSchemaVersions({
            variables: {
                urn,
                version1: version,
                version2: version - 1,
            },
        });
    };

    return (
        <SchemaContainer>
            {loading && <Message type="loading" content="Loading..." style={{ marginTop: '30%' }} />}
            <ViewRawButtonContainer padding={!showVersions ? 'true' : undefined}>
                {totalVersions > 0 &&
                    (!showVersions ? (
                        <ShowVersionButton onClick={() => setShowVersions(true)}>Version History</ShowVersionButton>
                    ) : (
                        <>
                            <ShowVersionButton
                                onClick={() => {
                                    setShowVersions(false);
                                    setCurrentVersion(0);
                                }}
                            >
                                Back
                            </ShowVersionButton>
                            <VersionText>Comparing version</VersionText>
                            <InputNumberContainer
                                autoFocus
                                bordered={false}
                                min={-totalVersions + 1}
                                max={0}
                                step={1}
                                value={currentVersion}
                                onChange={onVersionChange}
                            />
                            <VersionText> to version </VersionText>
                            <VersionRightText strong>{` ${currentVersion - 1}`}</VersionRightText>
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
                    <>
                        {showVersions && (
                            <SummaryContainer>
                                <li>
                                    <Typography.Text>{`minimum version is ${-totalVersions}`}</Typography.Text>
                                </li>
                                {diffSummary.added ? (
                                    <li>
                                        <Typography.Text>{`${diffSummary.added} column${
                                            diffSummary.added > 1 ? 's were' : ' was'
                                        } added`}</Typography.Text>
                                    </li>
                                ) : null}
                                {diffSummary.removed ? (
                                    <li>
                                        <Typography.Text>{`${diffSummary.removed} column${
                                            diffSummary.removed > 1 ? 's were' : ' was'
                                        } removed`}</Typography.Text>
                                    </li>
                                ) : null}
                                {diffSummary.updated ? (
                                    <li>
                                        <Typography.Text>{`${diffSummary.updated} description${
                                            diffSummary.updated > 1 ? 's were' : ' was'
                                        } updated`}</Typography.Text>
                                    </li>
                                ) : null}
                            </SummaryContainer>
                        )}
                        <SchemaTable
                            rows={rows}
                            editMode={!showVersions}
                            onUpdateDescription={onUpdateDescription}
                            onUpdateTags={onUpdateTags}
                            editableSchemaMetadata={editableSchemaMetadata}
                        />
                    </>
                )
            )}
        </SchemaContainer>
    );
}
