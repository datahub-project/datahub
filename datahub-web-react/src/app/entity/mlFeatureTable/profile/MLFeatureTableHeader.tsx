import { Row, Space, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { MlFeatureTable } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import CompactContext from '../../../shared/CompactContext';
import { AvatarsGroup } from '../../../shared/avatar';
import MarkdownViewer from '../../shared/MarkdownViewer';

const HeaderInfoItem = styled.div`
    display: inline-block;
    text-align: left;
    width: 125px;
    vertical-align: top;
`;

const PlatformName = styled(Typography.Text)`
    font-size: 16px;
`;

export type Props = {
    mlFeatureTable: MlFeatureTable;
};

export default function MLFeatureTableHeader({ mlFeatureTable: { platform, description, ownership } }: Props) {
    const entityRegistry = useEntityRegistry();
    const isCompact = React.useContext(CompactContext);

    return (
        <>
            <Space direction="vertical" size="middle">
                <Row justify="space-between">
                    <HeaderInfoItem>
                        <div>
                            <Typography.Text strong type="secondary" style={{ fontSize: 11 }}>
                                Platform
                            </Typography.Text>
                        </div>
                        <Space direction="horizontal">
                            {platform ? <PlatformName>{platform}</PlatformName> : null}
                        </Space>
                    </HeaderInfoItem>
                </Row>
                <MarkdownViewer isCompact={isCompact} source={description || ''} />
                <AvatarsGroup owners={ownership?.owners} entityRegistry={entityRegistry} size="large" />
            </Space>
        </>
    );
}
