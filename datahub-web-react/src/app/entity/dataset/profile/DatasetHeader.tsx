import { Badge, Image, Popover, Space, Typography } from 'antd';
import { FetchResult, MutationFunctionOptions } from '@apollo/client';
import styled from 'styled-components';
import React from 'react';

import { Dataset } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { AvatarsGroup } from '../../../shared/avatar';
import CompactContext from '../../../shared/CompactContext';
import { capitalizeFirstLetter } from '../../../shared/capitalizeFirstLetter';
import UpdatableDescription from '../../shared/UpdatableDescription';
import UsageFacepile from './UsageFacepile';

export type Props = {
    dataset: Dataset;
    updateDataset: (options?: MutationFunctionOptions<any, any> | undefined) => Promise<FetchResult>;
};

const HeaderInfoItem = styled.div`
    display: inline-block;
    text-align: left;
    width: 125px;
    vertical-align: top;
`;

const HeaderInfoItems = styled.div`
    display: inline-block;
    margin-top: -16px;
    vertical-align: top;
`;
const PreviewImage = styled(Image)`
    max-height: 20px;
    padding-top: 3px;
    width: auto;
    object-fit: contain;
`;

export default function DatasetHeader({
    dataset: { urn, type, description: originalDesc, ownership, deprecation, platform, editableProperties, usageStats },
    updateDataset,
}: Props) {
    const entityRegistry = useEntityRegistry();
    const isCompact = React.useContext(CompactContext);
    const platformName = capitalizeFirstLetter(platform.name);
    const platformLogoUrl = platform.info?.logoUrl;

    return (
        <>
            <Space direction="vertical" size="middle">
                <HeaderInfoItems>
                    <HeaderInfoItem>
                        <div>
                            <Typography.Text strong type="secondary" style={{ fontSize: 11 }}>
                                Platform
                            </Typography.Text>
                        </div>
                        <Space direction="horizontal">
                            {platformLogoUrl && (
                                <PreviewImage preview={false} src={platformLogoUrl} alt={platformName} />
                            )}
                            <Typography.Text style={{ fontSize: 16 }}>{platformName}</Typography.Text>
                        </Space>
                    </HeaderInfoItem>
                    {usageStats?.aggregations?.totalSqlQueries && (
                        <HeaderInfoItem>
                            <div>
                                <Typography.Text strong type="secondary" style={{ fontSize: 11 }}>
                                    Queries / month
                                </Typography.Text>
                            </div>
                            <span>
                                <Typography.Text style={{ fontSize: 16 }}>
                                    {usageStats?.aggregations?.totalSqlQueries}
                                </Typography.Text>
                            </span>
                        </HeaderInfoItem>
                    )}
                    {(usageStats?.aggregations?.users?.length || 0) > 0 && (
                        <HeaderInfoItem>
                            <div>
                                <Typography.Text strong type="secondary" style={{ fontSize: 11 }}>
                                    Top Users
                                </Typography.Text>
                            </div>
                            <div>
                                <UsageFacepile users={usageStats?.aggregations?.users} />
                            </div>
                        </HeaderInfoItem>
                    )}
                </HeaderInfoItems>
                <UpdatableDescription
                    isCompact={isCompact}
                    updateEntity={updateDataset}
                    updatedDescription={editableProperties?.description}
                    originalDescription={originalDesc}
                    urn={urn}
                    entityType={type}
                />
                <AvatarsGroup owners={ownership?.owners} entityRegistry={entityRegistry} size="large" />
                <div>
                    {deprecation?.deprecated && (
                        <Popover
                            placement="bottomLeft"
                            content={
                                <>
                                    <Typography.Paragraph>By: {deprecation?.actor}</Typography.Paragraph>
                                    {deprecation.decommissionTime && (
                                        <Typography.Paragraph>
                                            On: {new Date(deprecation?.decommissionTime).toUTCString()}
                                        </Typography.Paragraph>
                                    )}
                                    {deprecation?.note && (
                                        <Typography.Paragraph>{deprecation.note}</Typography.Paragraph>
                                    )}
                                </>
                            }
                            title="Deprecated"
                        >
                            <Badge count="Deprecated" />
                        </Popover>
                    )}
                </div>
            </Space>
        </>
    );
}
