import React, { useMemo } from 'react';
import { Table, Typography } from 'antd';
import { CheckSquareOutlined } from '@ant-design/icons';
import { AlignType } from 'rc-table/lib/interface';
import styled from 'styled-components';
import MlFeatureDataTypeIcon from './MlFeatureDataTypeIcon';
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
            return <MlFeatureDataTypeIcon dataType={dataType} />;
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
    {
        title: 'Primary Key',
        dataIndex: 'primaryKey',
        key: 'primaryKey',
        render: (_: any, record: MlFeature | MlPrimaryKey) =>
            record.__typename === 'MLPrimaryKey' ? <CheckSquareOutlined /> : null,
        width: 50,
    },
];

export default function MlFeatureTableSchema({ featureTableProperties }: Props) {
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
                    rowKey={(record) => `${record.dataType}-${record.name}`}
                    expandable={{ defaultExpandAllRows: true, expandRowByClick: true }}
                    pagination={false}
                />
            )}
        </SchemaContainer>
    );
}
