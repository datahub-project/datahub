import React from 'react';
import styled from 'styled-components';
import { Table, Typography } from 'antd';
import { GetMlModelGroupQuery } from '../../../../graphql/mlModelGroup.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { useBaseEntity } from '../../shared/EntityContext';
import { notEmpty } from '../../shared/utils';
import { EntityType } from '../../../../types.generated';
import { EmptyTab } from '../../shared/components/styled/EmptyTab';
import { InfoItem } from '../../shared/components/styled/InfoItem';


const InfoItemContainer = styled.div<{ justifyContent }>`
    display: flex;
    position: relative;
    justify-content: ${(props) => props.justifyContent};
    padding: 12px 2px 20px 2px;
`;

const InfoItemContent = styled.div`
    padding-top: 8px;
    width: 100px;
`;


const NameContainer = styled.div`
    display: flex;
    align-items: center;
`;

const NameLink = styled.a`
    font-weight: 700;
    color: inherit;
    font-size: 0.9rem;
    &:hover {
        color: #1890ff !important;
    }
`;

const TagContainer = styled.div`
    display: inline-flex;
    margin-left: 0px;
    margin-top: 3px;
    flex-wrap: wrap;
    margin-right: 8px;
    backgroundColor: white;
`;

const StyledTable = styled(Table)`
    &&& .ant-table-cell {
        padding: 16px;
    }
` as typeof Table;

const ModelsContainer = styled.div`
    width: 100%;
    padding: 20px;
`;

const VersionContainer = styled.div`
    display: flex;
    align-items: center;
`;

const AliasePill = styled.div`
    padding: 2px 8px;
    display: inline-flex;
    align-items: center;
    border-radius: 7px;
    border: 1px solid #d9d9d9;
    color: #595959;
    background: #fafafa;
    margin-right: 8px;
    margin-bottom: 4px;
`;

const TagPill = styled.div`
    padding: 2px 8px;
    display: inline-flex;
    align-items: center;
    border-radius: 7px;
    border: 1px solid #d9d9d9;
    color: #595959;
    background: white;
    margin-right: 8px;
    margin-bottom: 4px;
`;

export default function MLGroupModels() {
    const baseEntity = useBaseEntity<GetMlModelGroupQuery>();
    const entityRegistry = useEntityRegistry();

    const modelGroup = baseEntity?.mlModelGroup;
    
    const models = baseEntity?.mlModelGroup?.incoming?.relationships
        ?.map((relationship) => relationship.entity)
        .filter(notEmpty) || [];

    const formatDate = (timestamp?: number) => {
        if (!timestamp) return '-';
        const milliseconds = timestamp < 10000000000 ? timestamp * 1000 : timestamp;
        return new Date(milliseconds).toISOString().slice(0, 19).replace('T', ' ');
    };

    console.log("models", models);
    console.log("model properties", models[0].properties.customProperties[0]);

    const columns = [
        {
            title: 'Name',
            dataIndex: 'name',
            key: 'name',
            width: 300,
            render: (name, record) => (
                <NameContainer>
                    <NameLink href={entityRegistry.getEntityUrl(EntityType.Mlmodel, record.urn)}>
                        {name}
                    </NameLink>
                </NameContainer>
            ),
        },
        {
            title: 'Version',
            key: 'version',
            width: 70,
            render: (_: any, record: EntityType.Mlmodel) => (
                <VersionContainer>{record.properties?.modelVersion || '-'}</VersionContainer>
            ),
        },
        {
            title: 'Registered At',
            key: 'date',
            width: 150,
            render: (_: any, record: EntityType.Mlmodel) => (
                <Typography.Text>{formatDate(record.properties?.date)}</Typography.Text>
            ),
        },
        {
            title: 'Tags',
            key: 'tags',
            width: 200,
            render: (_: any, record: EntityType.Mlmodel) => {
                const tags = record.properties?.tags || [];

                return (
                <TagContainer>
                        {tags.map((tags, index) => (
                            <TagPill key={`${tags}-${index}`}>
                                {tags}
                            </TagPill>
                        ))}
                </TagContainer>
            );
        },
        },
        {
            title: 'Description',
            dataIndex: 'description',
            key: 'description',
            width: 300,
            render: (_: any, record: EntityType.Mlmodel) => {
                const editableDesc = record.editableProperties?.description;
                const originalDesc = record.description;
                
                return (
                    <Typography.Text>
                        {editableDesc || originalDesc || '-'}
                    </Typography.Text>
                );
            },
        },
    ];


    return (
        <ModelsContainer>
            <Typography.Title level={3}>Model Group Details</Typography.Title>
            <InfoItemContainer justifyContent="left">
                <InfoItem title="Created At">
                    <InfoItemContent>{modelGroup?.properties?.created?.time ? formatDate(modelGroup.properties.createdAt) : '-'}</InfoItemContent>
                </InfoItem>
                <InfoItem title="Last Modified At">
                    <InfoItemContent>{modelGroup?.properties?.lastModified ? formatDate(modelGroup.properties.lastModified) : '-'}</InfoItemContent>
                </InfoItem>
                {modelGroup?.properties?.created?.actor && (
                    <InfoItem title="Created By">
                        <InfoItemContent>{modelGroup.properties.created?.actor}</InfoItemContent>
                    </InfoItem>
                )}
            </InfoItemContainer>
            <Typography.Title level={3}>Models</Typography.Title>
            <StyledTable
                columns={columns}
                dataSource={models}
                pagination={false}
                rowKey="urn"
                expandable={{
                    defaultExpandAllRows: true,
                    expandRowByClick: true,
                }}
                locale={{
                    emptyText: (
                        <EmptyTab tab="mlModel" />
                    )
                }}
            />
        </ModelsContainer>
    );
}