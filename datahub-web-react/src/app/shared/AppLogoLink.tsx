import { Image } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled, { useTheme } from 'styled-components';
import { useAppConfig } from '../useAppConfig';
import { DEFAULT_APP_CONFIG } from '../../appConfigContext';

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
        <Link to="/">
            <LogoImage
                src={
                    appConfig.config !== DEFAULT_APP_CONFIG
                        ? appConfig.config.visualConfig.logoUrl || themeConfig.assets.logoUrl
                        : undefined
                }
                preview={false}
            />
        </Link>
    );
}
