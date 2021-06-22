import { Badge, Popover, Space, Typography } from 'antd';
import { FetchResult, MutationFunctionOptions } from '@apollo/client';
import styled from 'styled-components';
import React from 'react';

import { Dataset } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { AvatarsGroup } from '../../../shared/avatar';
import CompactContext from '../../../shared/CompactContext';
import { capitalizeFirstLetter } from '../../../shared/capitalizeFirstLetter';
import UpdatableDescription from '../../shared/UpdatableDescription';

export type Props = {
    dataset: Dataset;
    updateDataset: (options?: MutationFunctionOptions<any, any> | undefined) => Promise<FetchResult>;
};

const HeaderInfoItem = styled.div`
    display: inline-block;
    text-align: left;
    width: 125px;
`;

const HeaderInfoItems = styled.div`
    display: inline-block;
    margin-top: -16px;
`;

export default function DatasetHeader({
    dataset: { urn, type, description: originalDesc, ownership, deprecation, platform, editableProperties },
    updateDataset,
}: Props) {
    const entityRegistry = useEntityRegistry();
    const isCompact = React.useContext(CompactContext);
    const platformName = capitalizeFirstLetter(platform.name);

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
                        <Typography.Text style={{ fontSize: 16 }}>{platformName}</Typography.Text>
                    </HeaderInfoItem>
                    <HeaderInfoItem>
                        <div>
                            <Typography.Text strong type="secondary" style={{ fontSize: 11 }}>
                                Queries / week
                            </Typography.Text>
                        </div>
                        <span>
                            <Typography.Text style={{ fontSize: 16 }}>13.6k</Typography.Text>
                        </span>
                    </HeaderInfoItem>
                    <HeaderInfoItem>
                        <div>
                            <Typography.Text strong type="secondary" style={{ fontSize: 11 }}>
                                Users / week
                            </Typography.Text>
                        </div>
                        <span>
                            <Typography.Text style={{ fontSize: 16 }}>370</Typography.Text>
                        </span>
                    </HeaderInfoItem>
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
