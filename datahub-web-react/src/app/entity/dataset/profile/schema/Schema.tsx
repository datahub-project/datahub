import React, { useMemo, useState } from 'react';

import { Button, Table, Typography } from 'antd';
import { AlignType } from 'rc-table/lib/interface';

import TypeIcon from './TypeIcon';
import { Schema, SchemaFieldDataType, GlobalTags } from '../../../../../types.generated';
import TagGroup from '../../../../shared/TagGroup';

const ViewRawButtonContainer = styled.div`
    display: flex;
    justify-content: flex-end;
    padding-bottom: 16px;
`;

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

    const [showRaw, setShowRaw] = useState(false);

    return (
        <>
            {schema?.platformSchema?.__typename === 'TableSchema' && (
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
                <Table pagination={false} dataSource={schema?.fields} columns={columns} rowKey="fieldPath" />
            )}
        </>
    );
}
