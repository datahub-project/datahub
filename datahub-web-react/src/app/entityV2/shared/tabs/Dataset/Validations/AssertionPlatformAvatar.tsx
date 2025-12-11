/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Tooltip } from '@components';
import { Image, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { LinkWrapper } from '@app/shared/LinkWrapper';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { DataPlatform, EntityType } from '@types';

const PlatformContainer = styled.div<{ noRightMargin?: boolean }>`
    margin-right: ${(props) => (props.noRightMargin ? '0px' : '8px')};
`;

type Props = {
    platform: DataPlatform;
    externalUrl?: string;
    noRightMargin?: boolean;
};

export const AssertionPlatformAvatar = ({ platform, externalUrl, noRightMargin }: Props) => {
    const entityRegistry = useEntityRegistry();
    return (
        <Tooltip title={`Run by ${entityRegistry.getDisplayName(EntityType.DataPlatform, platform)}`}>
            <PlatformContainer noRightMargin={noRightMargin}>
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
