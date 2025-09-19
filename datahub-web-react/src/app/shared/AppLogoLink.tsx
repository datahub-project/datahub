import { Image } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled, { useTheme } from 'styled-components';

import { useAppConfig } from '@app/useAppConfig';
import { DEFAULT_APP_CONFIG } from '@src/appConfigContext';
import { resolveRuntimePath } from '@utils/runtimeBasePath';

const LogoImage = styled(Image)`
    display: inline-block;
    height: 32px;
    width: auto;
    margin-top: 2px;
`;

export default function AppLogoLink() {
    const appConfig = useAppConfig();
    const themeConfig = useTheme();

    return (
        <Link to={resolveRuntimePath('/')}>
            <LogoImage
                src={
                    appConfig.config !== DEFAULT_APP_CONFIG
                        ? resolveRuntimePath(appConfig.config.visualConfig.logoUrl || themeConfig.assets.logoUrl)
                        : undefined
                }
                preview={false}
            />
        </Link>
    );
}
