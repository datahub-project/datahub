import React, { useMemo } from 'react';

import { Table, Typography } from 'antd';
import { AlignType } from 'rc-table/lib/interface';

import TypeIcon from './TypeIcon';
import { Schema, SchemaFieldDataType, GlobalTags } from '../../../../../types.generated';
import TagGroup from '../../../../shared/TagGroup';

export type Props = {
    schema?: Schema | null;
};

const defaultColumns = [
    {
        width: 96,
        title: 'Type',
        dataIndex: 'type',
        key: 'type',
        align: 'center' as AlignType,
        render: (type: SchemaFieldDataType) => {
            return <TypeIcon type={type} />;
        },
    },
    {
        title: 'Field',
        dataIndex: 'fieldPath',
        key: 'fieldPath',
        render: (fieldPath: string) => {
            return <Typography.Text strong>{fieldPath}</Typography.Text>;
        },
    },
    {
        title: 'Description',
        dataIndex: 'description',
        key: 'description',
    },
];

const tagColumn = {
    title: 'Tags',
    dataIndex: 'globalTags',
    key: 'tag',
    render: (tags: GlobalTags) => {
        return <TagGroup globalTags={tags} />;
    },
};

export default function SchemaView({ schema }: Props) {
    const columns = useMemo(() => {
        const hasTags = schema?.fields?.some((field) => (field?.globalTags?.tags?.length || 0) > 0);

        return [...defaultColumns, ...(hasTags ? [tagColumn] : [])];
    }, [schema]);

    return <Table pagination={false} dataSource={schema?.fields} columns={columns} rowKey="fieldPath" />;
}
