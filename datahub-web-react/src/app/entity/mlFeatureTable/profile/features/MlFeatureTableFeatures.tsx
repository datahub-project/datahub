import React from 'react';
import { Table, Typography } from 'antd';
import { CheckSquareOutlined } from '@ant-design/icons';
import { AlignType } from 'rc-table/lib/interface';
import styled from 'styled-components';
import MlFeatureDataTypeIcon from './MlFeatureDataTypeIcon';
import { MlFeatureDataType, MlPrimaryKey, MlFeature } from '../../../../../types.generated';
import MarkdownViewer from '../../../shared/components/legacy/MarkdownViewer';
import { GetMlFeatureTableQuery } from '../../../../../graphql/mlFeatureTable.generated';
import { useBaseEntity } from '../../../shared/EntityContext';
import { notEmpty } from '../../../shared/utils';

const FeaturesContainer = styled.div`
    margin-bottom: 100px;
`;

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

export default function MlFeatureTableFeatures() {
    const baseEntity = useBaseEntity<GetMlFeatureTableQuery>();
    const featureTable = baseEntity?.mlFeatureTable;

    const features =
        featureTable?.featureTableProperties &&
        (featureTable?.featureTableProperties?.mlFeatures || featureTable?.featureTableProperties?.mlPrimaryKeys)
            ? [
                  ...(featureTable?.featureTableProperties?.mlPrimaryKeys || []),
                  ...(featureTable?.featureTableProperties?.mlFeatures || []),
              ].filter(notEmpty)
            : [];

    return (
        <FeaturesContainer>
            {features && features.length > 0 && (
                <Table
                    columns={defaultColumns}
                    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
                    // @ts-ignore
                    dataSource={features}
                    rowKey={(record) => `${record.dataType}-${record.name}`}
                    expandable={{ defaultExpandAllRows: true, expandRowByClick: true }}
                    pagination={false}
                />
            )}
        </FeaturesContainer>
    );
}
