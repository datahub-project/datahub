import React, { useState } from 'react';
import { Table, Typography } from 'antd';
import { AlignType } from 'rc-table/lib/interface';
import { markdownDiff } from 'markdown-diff';
import styled from 'styled-components';
import TypeIcon from './TypeIcon';
import {
    EditableSchemaMetadata,
    SchemaFieldDataType,
    GlobalTags,
    SchemaField,
    GlobalTagsUpdate,
    EditableSchemaFieldInfo,
    GlossaryTerms,
} from '../../../../../types.generated';
import { ExtendedSchemaFields } from '../../../shared/utils';
import TagTermGroup from '../../../../shared/tags/TagTermGroup';
import DescriptionField from './SchemaDescriptionField';

const MAX_FIELD_PATH_LENGTH = 100;

const LighterText = styled(Typography.Text)`
    color: rgba(0, 0, 0, 0.45);
`;

const defaultColumns = [
    {
        width: 100,
        title: 'Type',
        dataIndex: 'type',
        key: 'type',
        align: 'left' as AlignType,
        render: (type: SchemaFieldDataType, record: SchemaField) => {
            return <TypeIcon type={type} nativeDataType={record.nativeDataType} />;
        },
    },
    {
        title: 'Field',
        dataIndex: 'fieldPath',
        key: 'fieldPath',
        width: 100,
        render: (fieldPath: string) => {
            if (!fieldPath.includes('.')) {
                return <Typography.Text strong>{fieldPath}</Typography.Text>;
            }
            let [firstPath, lastPath] = fieldPath.split(/\.(?=[^.]+$)/);
            const isOverflow = fieldPath.length > MAX_FIELD_PATH_LENGTH;
            if (isOverflow) {
                if (lastPath.length >= MAX_FIELD_PATH_LENGTH) {
                    lastPath = `..${lastPath.substring(lastPath.length - MAX_FIELD_PATH_LENGTH)}`;
                    firstPath = '';
                } else {
                    firstPath = firstPath.substring(fieldPath.length - MAX_FIELD_PATH_LENGTH);
                    if (firstPath.includes('.')) {
                        firstPath = `..${firstPath.substring(firstPath.indexOf('.'))}`;
                    } else {
                        firstPath = '..';
                    }
                }
            }
            return (
                <span>
                    <LighterText>{`${firstPath}${lastPath ? '.' : ''}`}</LighterText>
                    {lastPath && <Typography.Text strong>{lastPath}</Typography.Text>}
                </span>
            );
        },
        filtered: true,
    },
];

export type Props = {
    rows: Array<ExtendedSchemaFields>;
    // pastRows: Array<ExtendedSchemaFields>;
    onUpdateDescription: (updatedDescription: string, record?: EditableSchemaFieldInfo) => Promise<any>;
    onUpdateTags: (update: GlobalTagsUpdate, record?: EditableSchemaFieldInfo) => Promise<any>;
    editableSchemaMetadata?: EditableSchemaMetadata | null;
    editMode?: boolean;
};

export default function SchemaTable({
    rows,
    // pastRows,
    onUpdateDescription,
    onUpdateTags,
    editableSchemaMetadata,
    editMode = true,
}: Props) {
    const [tagHoveredIndex, setTagHoveredIndex] = useState<string | undefined>(undefined);
    const descriptionRender = (description: string, record: ExtendedSchemaFields) => {
        const relevantEditableFieldInfo = editableSchemaMetadata?.editableSchemaFieldInfo.find(
            (candidateEditableFieldInfo) => candidateEditableFieldInfo.fieldPath === record.fieldPath,
        );
        const descriptionDiff = editMode ? description : markdownDiff(record.pastDescription || '', description);
        return (
            <DescriptionField
                description={editMode ? relevantEditableFieldInfo?.description || description : descriptionDiff}
                isEdited={!!relevantEditableFieldInfo?.description}
                onUpdate={(update) => onUpdateDescription(update, relevantEditableFieldInfo)}
                editable={editMode}
            />
        );
    };

    const tagAndTermRender = (tags: GlobalTags, record: SchemaField, rowIndex: number | undefined) => {
        const relevantEditableFieldInfo = editableSchemaMetadata?.editableSchemaFieldInfo.find(
            (candidateEditableFieldInfo) => candidateEditableFieldInfo.fieldPath === record.fieldPath,
        );
        return (
            <TagTermGroup
                uneditableTags={tags}
                editableTags={relevantEditableFieldInfo?.globalTags}
                glossaryTerms={record.glossaryTerms as GlossaryTerms}
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
        width: 300,
    };

    const tagAndTermColumn = {
        width: 150,
        title: 'Tags & Terms',
        dataIndex: 'globalTags',
        key: 'tag',
        render: tagAndTermRender,
        onCell: (record: SchemaField, rowIndex: number | undefined) => ({
            onMouseEnter: () => {
                if (editMode) {
                    setTagHoveredIndex(`${record.fieldPath}-${rowIndex}`);
                }
            },
            onMouseLeave: () => {
                if (editMode) {
                    setTagHoveredIndex(undefined);
                }
            },
        }),
    };

    return (
        <Table
            columns={[...defaultColumns, descriptionColumn, tagAndTermColumn]}
            dataSource={rows}
            rowKey="fieldPath"
            expandable={{ defaultExpandAllRows: true, expandRowByClick: true }}
            pagination={false}
        />
    );
}
