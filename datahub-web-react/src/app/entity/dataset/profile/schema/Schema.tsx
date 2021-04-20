import React, { useState, useEffect } from 'react';

import { Button, Table, Typography } from 'antd';
import { AlignType } from 'rc-table/lib/interface';
import styled from 'styled-components';
import { FetchResult } from '@apollo/client';

import TypeIcon from './TypeIcon';
import {
    Schema,
    SchemaFieldDataType,
    GlobalTags,
    EditableSchemaMetadata,
    SchemaField,
    EditableSchemaMetadataUpdate,
    GlobalTagsUpdate,
    EditableSchemaFieldInfo,
    EditableSchemaFieldInfoUpdate,
} from '../../../../../types.generated';
import TagGroup from '../../../../shared/tags/TagGroup';
import { UpdateDatasetMutation } from '../../../../../graphql/dataset.generated';
import { convertTagsForUpdate } from '../../../../shared/tags/utils/convertTagsForUpdate';
import DescriptionField from './SchemaDescriptionField';

const MAX_FIELDPATH_LENGTH = 100;
const ViewRawButtonContainer = styled.div`
    display: flex;
    justify-content: flex-end;
    padding-bottom: 16px;
`;

export type Props = {
    schema?: Schema | null;
    editableSchemaMetadata?: EditableSchemaMetadata | null;
    updateEditableSchema: (
        update: EditableSchemaMetadataUpdate,
    ) => Promise<FetchResult<UpdateDatasetMutation, Record<string, any>, Record<string, any>>>;
};

interface ExtendedSchemaFields extends SchemaField {
    children?: Array<SchemaField>;
}

const defaultColumns = [
    {
        width: 288,
        title: 'Type',
        dataIndex: 'type',
        key: 'type',
        align: 'left' as AlignType,
        render: (type: SchemaFieldDataType) => {
            return <TypeIcon type={type} />;
        },
    },
    {
        title: 'Field',
        dataIndex: 'fieldPath',
        key: 'fieldPath',
        width: 192,
        render: (fieldPath: string) => {
            let [firstPath, lastPath] = fieldPath.split(/\.(?=[^.]+$)/);
            const isOverflow = fieldPath.length > MAX_FIELDPATH_LENGTH;
            if (isOverflow) {
                if (lastPath.length >= MAX_FIELDPATH_LENGTH) {
                    lastPath = `..${lastPath.substring(lastPath.length - MAX_FIELDPATH_LENGTH)}`;
                    firstPath = '';
                } else {
                    firstPath = firstPath.substring(fieldPath.length - MAX_FIELDPATH_LENGTH);
                    if (firstPath.includes('.')) {
                        firstPath = `..${firstPath.substring(firstPath.indexOf('.'))}`;
                    } else {
                        firstPath = '..';
                    }
                }
            }
            return (
                <>
                    <Typography.Text>{`${firstPath}${lastPath ? '.' : ''}`}</Typography.Text>
                    {lastPath && <Typography.Text strong>{lastPath}</Typography.Text>}
                </>
            );
        },
    },
];

function convertEditableSchemaMetadataForUpdate(
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

export default function SchemaView({ schema, editableSchemaMetadata, updateEditableSchema }: Props) {
    const [tagHoveredIndex, setTagHoveredIndex] = useState<string | undefined>(undefined);
    const [descHoveredIndex, setDescHoveredIndex] = useState<string | undefined>(undefined);
    const [showRaw, setShowRaw] = useState(false);
    const [rows, setRows] = useState<Array<ExtendedSchemaFields>>([]);

    useEffect(() => {
        const fields = [...(schema?.fields || [])] as Array<ExtendedSchemaFields>;
        if (fields.length > 1) {
            // eslint-disable-next-line no-nested-ternary
            fields.sort((a, b) => (a.fieldPath > b.fieldPath ? 1 : b.fieldPath > a.fieldPath ? -1 : 0));
            for (let rowIndex = fields.length; rowIndex--; rowIndex >= 0) {
                const field = fields[rowIndex];
                if (field.fieldPath.slice(1, -1).includes('.')) {
                    const fieldPaths = field.fieldPath.split(/\.(?=[^.]+$)/);
                    const parentFieldIndex = fields.findIndex((f) => f.fieldPath === fieldPaths[0]);
                    if (parentFieldIndex > -1) {
                        if ('children' in fields[parentFieldIndex]) {
                            fields[parentFieldIndex].children?.unshift(field);
                        } else {
                            fields[parentFieldIndex] = { ...fields[parentFieldIndex], children: [field] };
                        }
                        fields.splice(rowIndex, 1);
                    } else if (rowIndex > 0 && fieldPaths[0].includes(fields[rowIndex - 1].fieldPath)) {
                        if ('children' in fields[rowIndex - 1]) {
                            fields[rowIndex - 1].children?.unshift(field);
                        } else {
                            fields[rowIndex - 1] = { ...fields[rowIndex - 1], children: [field] };
                        }
                        fields.splice(rowIndex, 1);
                    }
                }
            }
        }
        setRows(fields);
    }, [schema?.fields]);

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
        const newFieldInfo: EditableSchemaFieldInfoUpdate = {
            fieldPath: record?.fieldPath,
            description: record?.description,
            globalTags: update,
        };

        return updateSchema(newFieldInfo, record);
    };

    const onUpdateDescription = (updatedDescription: string, record?: EditableSchemaFieldInfo) => {
        if (!record) return Promise.resolve();
        const newFieldInfo: EditableSchemaFieldInfoUpdate = {
            fieldPath: record?.fieldPath,
            description: updatedDescription,
            globalTags: { tags: convertTagsForUpdate(record?.globalTags?.tags || []) },
        };
        return updateSchema(newFieldInfo, record);
    };

    const descriptionRender = (description: string, record: SchemaField, rowIndex: number | undefined) => {
        const relevantEditableFieldInfo = editableSchemaMetadata?.editableSchemaFieldInfo.find(
            (candidateEditableFieldInfo) => candidateEditableFieldInfo.fieldPath === record.fieldPath,
        ) || { fieldPath: record.fieldPath };
        return (
            <DescriptionField
                description={description}
                updatedDescription={relevantEditableFieldInfo.description}
                onHover={descHoveredIndex !== undefined && descHoveredIndex === `${record.fieldPath}-${rowIndex}`}
                onUpdate={(update) => onUpdateDescription(update, relevantEditableFieldInfo)}
            />
        );
    };

    const tagGroupRender = (tags: GlobalTags, record: SchemaField, rowIndex: number | undefined) => {
        const relevantEditableFieldInfo = editableSchemaMetadata?.editableSchemaFieldInfo.find(
            (candidateEditableFieldInfo) => candidateEditableFieldInfo.fieldPath === record.fieldPath,
        );
        return (
            <TagGroup
                uneditableTags={tags}
                editableTags={relevantEditableFieldInfo?.globalTags}
                canRemove
                canAdd={tagHoveredIndex === `${record.fieldPath}-${rowIndex}`}
                onOpenModal={() => setTagHoveredIndex(undefined)}
                updateTags={(update) =>
                    onUpdateTags(update, relevantEditableFieldInfo || { fieldPath: record.fieldPath })
                }
            />
        );
    };

    const descriptionColumn = {
        title: 'Description',
        dataIndex: 'description',
        key: 'description',
        render: descriptionRender,
        width: 700,
        onCell: (record: SchemaField, rowIndex: number | undefined) => ({
            onMouseEnter: () => {
                setDescHoveredIndex(`${record.fieldPath}-${rowIndex}`);
            },
            onMouseLeave: () => {
                setDescHoveredIndex(undefined);
            },
        }),
    };

    const tagColumn = {
        width: 400,
        title: 'Tags',
        dataIndex: 'globalTags',
        key: 'tag',
        render: tagGroupRender,
        onCell: (record: SchemaField, rowIndex: number | undefined) => ({
            onMouseEnter: () => {
                setTagHoveredIndex(`${record.fieldPath}-${rowIndex}`);
            },
            onMouseLeave: () => {
                setTagHoveredIndex(undefined);
            },
        }),
    };

    return (
        <>
            {schema?.platformSchema?.__typename === 'TableSchema' && schema?.platformSchema?.schema?.length > 0 && (
                <ViewRawButtonContainer>
                    <Button onClick={() => setShowRaw(!showRaw)}>{showRaw ? 'Tabular' : 'Raw'}</Button>
                </ViewRawButtonContainer>
            )}
            {showRaw ? (
                <Typography.Text data-testid="schema-raw-view">
                    <pre>
                        <code>
                            {schema?.platformSchema?.__typename === 'TableSchema' &&
                                JSON.stringify(JSON.parse(schema.platformSchema.schema), null, 2)}
                        </code>
                    </pre>
                </Typography.Text>
            ) : (
                <Table
                    pagination={false}
                    columns={[...defaultColumns, descriptionColumn, tagColumn]}
                    dataSource={rows}
                    rowKey="fieldPath"
                />
            )}
        </>
    );
}
