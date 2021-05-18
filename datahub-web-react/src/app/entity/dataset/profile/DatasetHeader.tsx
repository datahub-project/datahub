import { Badge, Divider, Popover, Space, Typography } from 'antd';
import { ParagraphProps } from 'antd/lib/typography/Paragraph';
import React from 'react';
import styled from 'styled-components';
import { Dataset } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { AvatarsGroup } from '../../../shared/avatar';
import CompactContext from '../../../shared/CompactContext';
import { capitalizeFirstLetter } from '../../../shared/capitalizeFirstLetter';

type DescriptionTextProps = ParagraphProps & {
    isCompact: boolean;
};

const DescriptionText = styled(({ isCompact: _, ...props }: DescriptionTextProps) => (
    <Typography.Paragraph {...props} />
))`
    ${(props) => (props.isCompact ? 'max-width: 377px;' : '')};
    display: block;
    overflow-wrap: break-word;
    word-wrap: break-word;
`;

export type Props = {
    dataset: Dataset;
};

export default function DatasetHeader({ dataset: { description, ownership, deprecation, platform } }: Props) {
    const entityRegistry = useEntityRegistry();
    const isCompact = React.useContext(CompactContext);
    const platformName = capitalizeFirstLetter(platform.name);

    return (
        <>
            <Space direction="vertical" size="middle">
                <Space split={<Divider type="vertical" />}>
                    <Typography.Text>Dataset</Typography.Text>
                    <Typography.Text strong>{platformName}</Typography.Text>
                </Space>
                <DescriptionText isCompact={isCompact}>{description}</DescriptionText>
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
