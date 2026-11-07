import { Avatar, Text, Tooltip } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { LinkWrapper } from '@app/sharedV2/LinkWrapper';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { DataPlatform, EntityType } from '@types';

const PlatformContainer = styled.div<{ noRightMargin?: boolean }>`
    margin-right: ${(props) => (props.noRightMargin ? '0px' : '8px')};
`;

const PlatformName = styled(Text).attrs({ type: 'span' })`
    color: ${(props) => props.theme.colors.text};
    font-size: inherit;
    line-height: inherit;
    word-break: break-word;
`;

type Props = {
    platform: DataPlatform;
    externalUrl?: string;
    noRightMargin?: boolean;
};

export const AssertionPlatformAvatar = ({ platform, externalUrl, noRightMargin }: Props) => {
    const { t } = useTranslation('entity.profile.validations');
    const entityRegistry = useEntityRegistry();
    return (
        <Tooltip
            title={t('profile.runByPlatform', {
                platformName: entityRegistry.getDisplayName(EntityType.DataPlatform, platform),
            })}
        >
            <PlatformContainer noRightMargin={noRightMargin}>
                <LinkWrapper to={externalUrl} target="_blank" onClick={(e) => e.stopPropagation()}>
                    {(platform.properties?.logoUrl && (
                        <Avatar
                            name={entityRegistry.getDisplayName(EntityType.DataPlatform, platform)}
                            imageUrl={platform.properties?.logoUrl}
                            size="md"
                        />
                    )) || (
                        <PlatformName>{entityRegistry.getDisplayName(EntityType.DataPlatform, platform)}</PlatformName>
                    )}
                </LinkWrapper>
            </PlatformContainer>
        </Tooltip>
    );
};
