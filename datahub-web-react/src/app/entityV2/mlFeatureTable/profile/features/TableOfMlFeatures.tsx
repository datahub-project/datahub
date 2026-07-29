import { CheckSquareOutlined } from '@ant-design/icons';
import { Table, Typography } from 'antd';
import { AlignType } from 'rc-table/lib/interface';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { useRefetch } from '@app/entity/shared/EntityContext';
import SchemaDescriptionField from '@app/entityV2/dataset/profile/schema/components/SchemaDescriptionField';
import MlFeatureDataTypeIcon from '@app/entityV2/mlFeatureTable/profile/features/MlFeatureDataTypeIcon';
import TagTermGroup from '@app/sharedV2/tags/TagTermGroup';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useUpdateDescriptionMutation } from '@graphql/mutations.generated';
import { MlFeature, MlFeatureDataType, MlPrimaryKey } from '@types';

const FeaturesContainer = styled.div`
    margin-bottom: 100px;
`;

type Props = {
    features: Array<MlFeature | MlPrimaryKey>;
};

export default function TableOfMlFeatures({ features }: Props) {
    const refetch = useRefetch();
    const [updateDescription] = useUpdateDescriptionMutation();
    const entityRegistry = useEntityRegistry();
    const { t } = useTranslation('entity.types');
    const { t: tl } = useTranslation('common.labels');

    const defaultColumns = [
        {
            title: tl('type'),
            dataIndex: 'dataType',
            key: 'dataType',
            width: 100,
            align: 'left' as AlignType,
            render: (dataType: MlFeatureDataType) => {
                return <MlFeatureDataTypeIcon dataType={dataType} />;
            },
        },
    ];

    const [tagHoveredIndex, setTagHoveredIndex] = useState<string | undefined>(undefined);
    const [expandedRows, setExpandedRows] = useState({});

    const onTagTermCell = (record: any, rowIndex: number | undefined) => ({
        onMouseEnter: () => {
            setTagHoveredIndex(`${record.urn}-${rowIndex}`);
        },
        onMouseLeave: () => {
            setTagHoveredIndex(undefined);
        },
    });

    const nameColumn = {
        title: tl('name'),
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
        title: tl('description'),
        dataIndex: 'description',
        key: 'description',
        render: (_, feature: MlFeature | MlPrimaryKey, index: number) => (
            <SchemaDescriptionField
                onExpanded={(expanded) => {
                    setExpandedRows((prev) => ({ ...prev, [index]: expanded }));
                }}
                expanded={!!expandedRows[index]}
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
        title: tl('tags'),
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
        title: t('shared.termsColumn'),
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
        title: t('mlFeatureTable.primaryKeyColumn'),
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
                    data-testid="mlmodel-features-table"
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
