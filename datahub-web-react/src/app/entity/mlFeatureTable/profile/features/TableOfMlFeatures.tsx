import React, { useState } from 'react';
import { Table, Typography } from 'antd';
import { CheckSquareOutlined } from '@ant-design/icons';
import { AlignType } from 'rc-table/lib/interface';
import styled from 'styled-components';
import { Link } from 'react-router-dom';

import MlFeatureDataTypeIcon from './MlFeatureDataTypeIcon';
import { MlFeatureDataType, MlPrimaryKey, MlFeature } from '../../../../../types.generated';
import { useRefetch } from '../../../shared/EntityContext';
import TagTermGroup from '../../../../shared/tags/TagTermGroup';
import SchemaDescriptionField from '../../../dataset/profile/schema/components/SchemaDescriptionField';
import { useUpdateDescriptionMutation } from '../../../../../graphql/mutations.generated';
import { useEntityRegistry } from '../../../../useEntityRegistry';

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
];

type Props = {
    features: Array<MlFeature | MlPrimaryKey>;
};

export default function TableOfMlFeatures({ features }: Props) {
    const refetch = useRefetch();
    const [updateDescription] = useUpdateDescriptionMutation();
    const entityRegistry = useEntityRegistry();

    const [tagHoveredIndex, setTagHoveredIndex] = useState<string | undefined>(undefined);

    const onTagTermCell = (record: any, rowIndex: number | undefined) => ({
        onMouseEnter: () => {
            setTagHoveredIndex(`${record.urn}-${rowIndex}`);
        },
        onMouseLeave: () => {
            setTagHoveredIndex(undefined);
        },
    });

    const nameColumn = {
        title: 'Name',
        dataIndex: 'name',
        key: 'name',
        width: 100,
        render: (name: string, feature: MlFeature | MlPrimaryKey) => (
            <Link to={entityRegistry.getEntityUrl(feature.type, feature.urn)}>
                <Typography.Text strong>{name}</Typography.Text>
            </Link>
        ),
    };

    const descriptionColumn = {
        title: 'Description',
        dataIndex: 'description',
        key: 'description',
        render: (_, feature: MlFeature | MlPrimaryKey) => (
            <SchemaDescriptionField
                description={feature?.editableProperties?.description || feature?.properties?.description || ''}
                original={feature?.properties?.description}
                isEdited={!!feature?.editableProperties?.description}
                onUpdate={(updatedDescription) =>
                    updateDescription({
                        variables: {
                            input: {
                                description: updatedDescription,
                                resourceUrn: feature.urn,
                            },
                        },
                    }).then(refetch)
                }
            />
        ),
        width: 300,
    };

    const tagColumn = {
        width: 125,
        title: 'Tags',
        dataIndex: 'tags',
        key: 'tags',
        render: (_, feature: MlFeature | MlPrimaryKey, rowIndex: number) => (
            <TagTermGroup
                editableTags={feature.tags}
                canRemove
                buttonProps={{ size: 'small' }}
                canAddTag={tagHoveredIndex === `${feature.urn}-${rowIndex}`}
                onOpenModal={() => setTagHoveredIndex(undefined)}
                entityUrn={feature.urn}
                entityType={feature.type}
                refetch={refetch}
            />
        ),
        onCell: onTagTermCell,
    };

    const termColumn = {
        width: 125,
        title: 'Terms',
        dataIndex: 'glossaryTerms',
        key: 'glossaryTerms',
        render: (_, feature: MlFeature | MlPrimaryKey, rowIndex: number) => (
            <TagTermGroup
                editableGlossaryTerms={feature.glossaryTerms}
                canRemove
                buttonProps={{ size: 'small' }}
                canAddTerm={tagHoveredIndex === `${feature.urn}-${rowIndex}`}
                onOpenModal={() => setTagHoveredIndex(undefined)}
                entityUrn={feature.urn}
                entityType={feature.type}
                refetch={refetch}
            />
        ),
        onCell: onTagTermCell,
    };

    const primaryKeyColumn = {
        title: 'Primary Key',
        dataIndex: 'primaryKey',
        key: 'primaryKey',
        render: (_: any, record: MlFeature | MlPrimaryKey) =>
            record.__typename === 'MLPrimaryKey' ? <CheckSquareOutlined /> : null,
        width: 50,
    };

    const allColumns = [...defaultColumns, nameColumn, descriptionColumn, tagColumn, termColumn, primaryKeyColumn];

    return (
        <FeaturesContainer>
            {features && features.length > 0 && (
                <Table
                    columns={allColumns}
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
