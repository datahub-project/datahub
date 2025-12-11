/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Image } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled, { useTheme } from 'styled-components';

import { useAppConfig } from '@app/useAppConfig';
import { DEFAULT_APP_CONFIG } from '@src/appConfigContext';

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
