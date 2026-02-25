import React from 'react';
import styled, { CSSObject, useTheme } from 'styled-components';

import StackPlatformImages from '@app/entityV2/shared/containers/profile/header/PlatformContent/StackPlatformImages';
import PlatformIcon from '@app/sharedV2/icons/PlatformIcon';

import { DataPlatform } from '@types';

const LogoIcon = styled.span`
    display: flex;
    gap: 4px;
    margin-right: 4px;
`;

const PlatformContentWrapper = styled.div`
    display: flex;
    align-items: center;
    flex-wrap: nowrap;
`;

interface Props {
    platform?: DataPlatform;
    platforms?: DataPlatform[];
    size?: number;
    styles?: CSSObject;
}

function PlatformHeaderIcons(props: Props) {
    const { platform, platforms, size = 28, styles } = props;
    const theme = useTheme();
    const iconStyles = {
        borderRadius: '16px',
        border: `1px solid ${theme.colors.bg}`,
        padding: '10px',
    };

    return (
        <PlatformContentWrapper>
            {platform && (
                <LogoIcon>
                    {!platforms && (
                        <PlatformIcon
                            platform={platform}
                            size={size}
                            styles={styles ? { ...iconStyles, ...styles } : iconStyles}
                        />
                    )}
                    {!!platforms && <StackPlatformImages platforms={platforms} size={size} styles={styles} />}
                </LogoIcon>
            )}
        </PlatformContentWrapper>
    );
}

export default PlatformHeaderIcons;
