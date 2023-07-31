import React from 'react';
import styled from 'styled-components';
import { Tooltip, Typography, Image } from 'antd';
import { DataPlatform, EntityType } from '../../../../../../types.generated';
import { LinkWrapper } from '../../../../../shared/LinkWrapper';
import { useEntityRegistry } from '../../../../../useEntityRegistry';

const PlatformContainer = styled.div`
    margin-right: 8px;
`;

type Props = {
    platform: DataPlatform;
    lastEvaluationUrl?: string;
};

export const AssertionPlatformAvatar = ({ platform, lastEvaluationUrl }: Props) => {
    const entityRegistry = useEntityRegistry();
    return (
        <Tooltip title={entityRegistry.getDisplayName(EntityType.DataPlatform, platform)}>
            <PlatformContainer>
                {(platform.properties?.logoUrl && (
                    <LinkWrapper to={lastEvaluationUrl} target="_blank" onClick={(e) => e.stopPropagation()}>
                        <Image preview={false} height={20} width={20} src={platform.properties?.logoUrl} />
                    </LinkWrapper>
                )) || (
                    <Typography.Text>
                        {entityRegistry.getDisplayName(EntityType.DataPlatform, platform)}
                    </Typography.Text>
                )}
            </PlatformContainer>
        </Tooltip>
    );
};
