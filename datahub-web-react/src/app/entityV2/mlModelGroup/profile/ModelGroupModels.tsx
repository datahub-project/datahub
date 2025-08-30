import { Table, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import StripMarkdownText from '@app/entity/shared/components/styled/StripMarkdownText';
import { EmptyTab } from '@app/entityV2/shared/components/styled/EmptyTab';
import { InfoItem } from '@app/entityV2/shared/components/styled/InfoItem';
import { notEmpty } from '@app/entityV2/shared/utils';
import { TimestampPopover } from '@app/sharedV2/TimestampPopover';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Pill } from '@src/alchemy-components/components/Pills';
import { colors } from '@src/alchemy-components/theme';

import { GetMlModelGroupQuery } from '@graphql/mlModelGroup.generated';
import { EntityType } from '@types';

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
    const [expandedDescriptions, setExpandedDescriptions] = useState<Set<string>>(new Set());

    const ABBREVIATED_LIMIT = 80;

    const handleExpanded = (urn: string, expanded: boolean) => {
        const newExpanded = new Set(expandedDescriptions);
        if (expanded) {
            newExpanded.add(urn);
        } else {
            newExpanded.delete(urn);
        }
        setExpandedDescriptions(newExpanded);
    };

    const models =
        baseEntity?.mlModelGroup?.incoming?.relationships
            ?.map((relationship) => relationship.entity)
            .filter(notEmpty)
            // eslint-disable-next-line @typescript-eslint/dot-notation
            ?.sort((a, b) => b?.['properties']?.createdTS?.time - a?.['properties']?.createdTS?.time) || [];

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
                <TimestampPopover timestamp={record.properties?.createdTS?.time} title="Created At" showPopover />
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
                            <Pill key={alias.versionTag} label={alias.versionTag} color="blue" clickable={false} />
                        ))}
                    </TagContainer>
                );
            },
        },
        {
            title: 'Properties',
            key: 'properties',
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
                const description = editableDesc || originalDesc || '';
                const isExpanded = expandedDescriptions.has(record.urn);

                if (!description) {
                    return <Typography.Text>-</Typography.Text>;
                }

                if (isExpanded) {
                    return (
                        <>
                            <Typography.Text>{description}</Typography.Text>
                            <br />
                            <Typography.Link
                                onClick={(e) => {
                                    e.stopPropagation();
                                    handleExpanded(record.urn, false);
                                }}
                            >
                                Read Less
                            </Typography.Link>
                        </>
                    );
                }

                return (
                    <StripMarkdownText
                        limit={ABBREVIATED_LIMIT}
                        readMore={
                            <>
                                <Typography.Link
                                    onClick={(e) => {
                                        e.stopPropagation();
                                        handleExpanded(record.urn, true);
                                    }}
                                >
                                    Read More
                                </Typography.Link>
                            </>
                        }
                        shouldWrap
                    >
                        {description}
                    </StripMarkdownText>
                );
            },
        },
    ];

    return (
        <ModelsContainer>
            <Typography.Title level={3}>Model Group Details</Typography.Title>
            <InfoItemContainer justifyContent="left">
                <InfoItem title="Created At">
                    <TimestampPopover timestamp={modelGroup?.properties?.created?.time} title="Created At" />
                </InfoItem>
                <InfoItem title="Last Modified At">
                    <TimestampPopover timestamp={modelGroup?.properties?.lastModified?.time} title="Last Modified At" />
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
