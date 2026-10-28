import { Image } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled, { useTheme } from 'styled-components';

import { useAppConfig } from '@app/useAppConfig';
import { PageRoutes } from '@conf/Global';
import { DEFAULT_APP_CONFIG } from '@src/appConfigContext';

const StyledLink = styled(Link)`
    display: flex;
`;

const LogoImage = styled(Image)`
    display: inline-block;
    height: 32px;
    width: auto;
    display: flex;
`;

export default function AppLogoLink() {
    const appConfig = useAppConfig();
    const themeConfig = useTheme();

    return (
        <StyledLink to={PageRoutes.HOME}>
            <LogoImage
                src={
                    appConfig.config !== DEFAULT_APP_CONFIG
                        ? appConfig.config.visualConfig.logoUrl || themeConfig.assets.logoUrl
                        : undefined
                }
                preview={false}
            />
        </StyledLink>
    );
}
