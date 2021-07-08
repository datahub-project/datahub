import React, { useMemo } from 'react';

import { Table, Typography } from 'antd';
import { AlignType } from 'rc-table/lib/interface';
import styled from 'styled-components';

import TypeIcon from './TypeIcon';
import { MlFeatureDataType, MlFeatureTableProperties, MlPrimaryKey, MlFeature } from '../../../../../types.generated';
import { notEmpty } from '../../../shared/utils';
import MarkdownViewer from '../../../shared/MarkdownViewer';

const SchemaContainer = styled.div`
    margin-bottom: 100px;
`;

export type Props = {
    featureTableProperties?: MlFeatureTableProperties | null;
};

const defaultColumns = [
    {
        title: 'Type',
        dataIndex: 'dataType',
        key: 'dataType',
        width: 100,
        align: 'left' as AlignType,
        render: (dataType: MlFeatureDataType) => {
            return <TypeIcon type={dataType} />;
        },
    },
    {
        title: 'Name',
        dataIndex: 'name',
        key: 'name',
        width: 100,
        render: (name: string) => <Typography.Text strong>{name}</Typography.Text>,
    },
    {
        title: 'Description',
        dataIndex: 'description',
        key: 'description',
        render: (description: string) => <MarkdownViewer source={description} />,
        width: 300,
    },
];

export default function SchemaView({ featureTableProperties }: Props) {
    const rows: Array<MlFeature | MlPrimaryKey> = useMemo(() => {
        if (featureTableProperties && (featureTableProperties?.mlFeatures || featureTableProperties?.mlPrimaryKeys)) {
            return [
                ...(featureTableProperties?.mlFeatures || []),
                ...(featureTableProperties?.mlPrimaryKeys || []),
            ].filter(notEmpty);
        }
        return [];
    }, [featureTableProperties]);

    return (
        <SchemaContainer>
            {rows.length > 0 && (
                <Table
                    columns={defaultColumns}
                    dataSource={rows}
                    rowKey="fieldPath"
                    expandable={{ defaultExpandAllRows: true, expandRowByClick: true }}
                    pagination={false}
                />
            )}
        </SchemaContainer>
    );
}
