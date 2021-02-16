import React, { useMemo } from 'react';

import { Table, Typography } from 'antd';
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

    return <Table pagination={false} dataSource={schema?.fields} columns={columns} />;
}
