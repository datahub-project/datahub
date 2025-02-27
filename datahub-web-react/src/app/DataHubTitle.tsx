import React from 'react';
import { Helmet } from 'react-helmet-async';
import { useLocation } from 'react-router';
import { useCustomTheme } from '../customThemeContext';
import { useAppConfig } from './useAppConfig';

const PATH_FRAGMENT_TO_TITLE_OVERRIDES = {
    sso: 'SSO',
    oidc: 'OIDC',
};

export default function DataHubTitle() {
    const location = useLocation();
    const { config } = useAppConfig();
    const { theme } = useCustomTheme();

    const title =
        location.pathname
            .split('/')
            .filter((word) => word !== '')
            .map((rawWord) => {
                if (rawWord in PATH_FRAGMENT_TO_TITLE_OVERRIDES) {
                    return PATH_FRAGMENT_TO_TITLE_OVERRIDES[rawWord];
                }
                // ie. personal-notifications -> Personal Notifications
                const words = rawWord.split('-');
                return words.map((word) => word.charAt(0).toUpperCase() + word.slice(1)).join(' ');
            })
            .join(' | ') ||
        config?.visualConfig?.appTitle ||
        theme?.content?.title;

    if (!title) return null;
    return (
        <Helmet>
            <title>{title}</title>
        </Helmet>
    );
}
