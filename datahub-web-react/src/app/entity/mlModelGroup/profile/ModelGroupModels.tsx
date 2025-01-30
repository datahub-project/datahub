import { Typography, Table } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { colors } from '@src/alchemy-components/theme';
import { Pill } from '@src/alchemy-components/components/Pills';
import moment from 'moment';
import { GetMlModelGroupQuery } from '../../../../graphql/mlModelGroup.generated';
import { EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { useBaseEntity } from '../../shared/EntityContext';
import { notEmpty } from '../../shared/utils';
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
        color: ${colors.blue[400]} !important;
    }
`;

const TagContainer = styled.div`
    display: inline-flex;
    margin-left: 0px;
    margin-top: 3px;
    flex-wrap: wrap;
    margin-right: 8px;
    backgroundcolor: white;
    gap: 5px;
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

export default function MLGroupModels() {
    const baseEntity = useBaseEntity<GetMlModelGroupQuery>();
    const entityRegistry = useEntityRegistry();
    const modelGroup = baseEntity?.mlModelGroup;

    const models =
        baseEntity?.mlModelGroup?.incoming?.relationships
            ?.map((relationship) => relationship.entity)
            .filter(notEmpty) || [];

    const columns = [
        {
            title: 'Name',
            dataIndex: 'name',
            key: 'name',
            width: 300,
            render: (_: any, record) => (
                <NameContainer>
                    <NameLink href={entityRegistry.getEntityUrl(EntityType.Mlmodel, record.urn)}>
                        {record?.properties?.propertiesName || record?.name}
                    </NameLink>
                </NameContainer>
            ),
        },
        {
            title: 'Version',
            key: 'version',
            width: 70,
            render: (_: any, record: any) => (
                <VersionContainer>{record.versionProperties?.version?.versionTag || '-'}</VersionContainer>
            ),
        },
        {
            title: 'Created At',
            key: 'createdAt',
            width: 150,
            render: (_: any, record: any) => (
                <Typography.Text>
                    {record.properties?.createdTS?.time
                        ? moment(record.properties.createdTS.time).format('YYYY-MM-DD HH:mm:ss')
                        : '-'}
                </Typography.Text>
            ),
        },
        {
            title: 'Aliases',
            key: 'aliases',
            width: 200,
            render: (_: any, record: any) => {
                const aliases = record.versionProperties?.aliases || [];

                return (
                    <TagContainer>
                        {aliases.map((alias) => (
                            <Pill
                                key={alias.versionTag}
                                label={alias.versionTag}
                                colorScheme="blue"
                                clickable={false}
                            />
                        ))}
                    </TagContainer>
                );
            },
        },
        {
            title: 'Tags',
            key: 'tags',
            width: 200,
            render: (_: any, record: any) => {
                const tags = record.properties?.tags || [];

                return (
                    <TagContainer>
                        {tags.map((tag) => (
                            <Pill key={tag} label={tag} clickable={false} />
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
            render: (_: any, record: any) => {
                const editableDesc = record.editableProperties?.description;
                const originalDesc = record.description;

                return <Typography.Text>{editableDesc || originalDesc || '-'}</Typography.Text>;
            },
        },
    ];

    return (
        <ModelsContainer>
            <Typography.Title level={3}>Model Group Details</Typography.Title>
            <InfoItemContainer justifyContent="left">
                <InfoItem title="Created At">
                    <InfoItemContent>
                        {modelGroup?.properties?.created?.time
                            ? moment(modelGroup.properties.created.time).format('YYYY-MM-DD HH:mm:ss')
                            : '-'}
                    </InfoItemContent>
                </InfoItem>
                <InfoItem title="Last Modified At">
                    <InfoItemContent>
                        {modelGroup?.properties?.lastModified?.time
                            ? moment(modelGroup.properties.lastModified.time).format('YYYY-MM-DD HH:mm:ss')
                            : '-'}
                    </InfoItemContent>
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
                    emptyText: <EmptyTab tab="mlModel" />,
                }}
            />
        </ModelsContainer>
    );
}
