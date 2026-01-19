import React from 'react';
import { Helmet } from 'react-helmet-async';

import { useCustomTheme } from '@src/customThemeContext';

export const ExternalScriptLoader: React.FC = () => {
    const { theme } = useCustomTheme();
    const scripts = theme?.assets?.externalScripts || [];

    if (scripts.length === 0) {
        return null;
    }

    return (
        <Helmet>
            {scripts.map((scriptUrl) => (
                <script
                    key={scriptUrl}
                    src={scriptUrl}
                    async
                    onError={(e) => console.error(`Failed to load external script: ${scriptUrl}`, e)}
                />
            ))}
        </Helmet>
    );
};
