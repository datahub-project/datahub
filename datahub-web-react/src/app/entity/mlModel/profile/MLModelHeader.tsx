import { Image, Row, Space, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { MlModel } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import CompactContext from '../../../shared/CompactContext';
import { AvatarsGroup } from '../../../shared/avatar';
import MarkdownViewer from '../../shared/components/legacy/MarkdownViewer';

const HeaderInfoItem = styled.div`
    display: inline-block;
    text-align: left;
    width: 125px;
    vertical-align: top;
`;

const PlatformName = styled(Typography.Text)`
    font-size: 16px;
`;
const PreviewImage = styled(Image)`
    max-height: 20px;
    padding-top: 3px;
    width: auto;
    object-fit: contain;
`;

export type Props = {
    mlModel: MlModel;
};

export default function MLModelHeader({ mlModel: { ownership, platform, properties } }: Props) {
    const entityRegistry = useEntityRegistry();
    const isCompact = React.useContext(CompactContext);

    return (
        <>
            <Space direction="vertical" size="middle">
                <Row justify="space-between">
                    {platform ? (
                        <HeaderInfoItem>
                            <div>
                                <Typography.Text strong type="secondary" style={{ fontSize: 11 }}>
                                    Platform
                                </Typography.Text>
                            </div>
                            <Space direction="horizontal">
                                {platform.info?.logoUrl ? (
                                    <PreviewImage
                                        preview={false}
                                        src={platform.info?.logoUrl}
                                        placeholder
                                        alt={platform.name}
                                    />
                                ) : null}
                                <PlatformName>{platform.name}</PlatformName>
                            </Space>
                        </HeaderInfoItem>
                    ) : null}
                </Row>
                <MarkdownViewer isCompact={isCompact} source={properties?.description || ''} />
                <AvatarsGroup owners={ownership?.owners} entityRegistry={entityRegistry} />
            </Space>
        </>
    );
}
