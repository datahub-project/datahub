import React from 'react';
import styled from 'styled-components';
import { Typography, Image } from 'antd';
import { Tooltip } from '@components';
import { DataPlatform, EntityType } from '../../../../../../types.generated';
import { LinkWrapper } from '../../../../../shared/LinkWrapper';
import { useEntityRegistry } from '../../../../../useEntityRegistry';

const PlatformContainer = styled.div`
    margin-right: 8px;
`;

type Props = {
    platform: DataPlatform;
    externalUrl?: string;
};

export const AssertionPlatformAvatar = ({ platform, externalUrl }: Props) => {
    const entityRegistry = useEntityRegistry();
    return (
        <Tooltip title={`Run by ${entityRegistry.getDisplayName(EntityType.DataPlatform, platform)}`}>
            <PlatformContainer>
                <LinkWrapper to={externalUrl} target="_blank" onClick={(e) => e.stopPropagation()}>
                    {(platform.properties?.logoUrl && (
                        <Image
                            preview={false}
                            height={24}
                            width={24}
                            src={platform.properties?.logoUrl}
                            style={{ objectFit: 'fill', borderRadius: 12 }}
                        />
                    )) || (
                        <Typography.Text>
                            {entityRegistry.getDisplayName(EntityType.DataPlatform, platform)}
                        </Typography.Text>
                    )}
                </LinkWrapper>
            </PlatformContainer>
        </Tooltip>
    );
};
