import React from 'react';
import { Helmet } from 'react-helmet-async';
import { useLocation } from 'react-router';
import { useCustomTheme } from '../customThemeContext';
import { useAppConfig } from './useAppConfig';

export default function DataHubTitle() {
    const location = useLocation();
    const { config } = useAppConfig();
    const { theme } = useCustomTheme();

    const title =
        location.pathname
            .split('/')
            .filter((word) => word !== '')
            .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
            .join(' | ') ||
        config?.visualConfig?.appTitle ||
        theme?.content.title;

    if (!title) return null;
    return (
        <Helmet>
            <title>{title}</title>
        </Helmet>
    );
}
