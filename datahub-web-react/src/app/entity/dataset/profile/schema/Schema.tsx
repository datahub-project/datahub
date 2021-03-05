import React, { useMemo, useState } from 'react';

import { Button, Table, Typography } from 'antd';
import { AlignType } from 'rc-table/lib/interface';
import styled from 'styled-components';

// TODO(Gabe): Create these types in the graph and remove the mock tag types
import { Tag, TaggedSchemaField } from '../stories/sampleSchema';
import TypeIcon from './TypeIcon';
import SchemaTags from './SchemaTags';
import { Schema, SchemaField, SchemaFieldDataType } from '../../../../../types.generated';

const BadgeGroup = styled.div`
    margin-top: 4px;
    margin-left: -4px;
`;

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
        render: (fieldPath: string, row: SchemaField) => {
            const { tags = [] } = row as TaggedSchemaField;
            const descriptorTags = tags.filter((tag) => tag.descriptor);
            return (
                <>
                    <Typography.Text strong>{fieldPath}</Typography.Text>
                    <BadgeGroup>
                        <SchemaTags tags={descriptorTags} />
                    </BadgeGroup>
                </>
            );
        },
    },
    {
        title: 'Description',
        dataIndex: 'description',
        key: 'description',
    },
];

export default function SchemaView({ schema }: Props) {
    const columns = useMemo(() => {
        const distinctTagCategories = Array.from(
            new Set(
                schema?.fields
                    .flatMap((field) => (field as TaggedSchemaField).tags)
                    .map((tag) => !tag?.descriptor && tag?.category)
                    .filter(Boolean),
            ),
        );

        const categoryColumns = distinctTagCategories.map((category) => ({
            title: category,
            dataIndex: 'tags',
            key: `tag-${category}`,
            render: (tags: Tag[] = []) => {
                return <SchemaTags tags={tags.filter((tag) => tag.category === category)} />;
            },
        }));
        return [...defaultColumns, ...categoryColumns];
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
