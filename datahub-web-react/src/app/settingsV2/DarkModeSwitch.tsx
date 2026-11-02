import { Switch } from '@components';
import React, { useCallback, useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useTheme } from 'styled-components';

import { useIsDarkMode } from '@app/theme/useIsDarkMode';

// Custom themes can ignore the dark preference, so theme.id may never flip. Re-enable the
// switch on a timer regardless so it can never get stuck.
const RESET_TIMEOUT_MS = 2000;

export default function DarkModeSwitch() {
    const { t } = useTranslation('settings.preferences');
    const theme = useTheme();
    const [isDarkMode, toggleDarkMode] = useIsDarkMode();
    const [isSwitchingTheme, setIsSwitchingTheme] = useState(false);

    // Allow the disabled state to paint before the theme swap blocks the main thread.
    const handleToggleDarkMode = useCallback(() => {
        setIsSwitchingTheme(true);
        requestAnimationFrame(() => requestAnimationFrame(toggleDarkMode));
    }, [toggleDarkMode]);

    useEffect(() => {
        setIsSwitchingTheme(false);
    }, [theme.id]);

    useEffect(() => {
        if (!isSwitchingTheme) return undefined;
        const timeout = window.setTimeout(() => setIsSwitchingTheme(false), RESET_TIMEOUT_MS);
        return () => window.clearTimeout(timeout);
    }, [isSwitchingTheme]);

    return (
        <Switch
            label={t('darkMode.title')}
            labelStyle={{
                position: 'absolute',
                width: 1,
                height: 1,
                overflow: 'hidden',
                clip: 'rect(0 0 0 0)',
            }}
            isChecked={isDarkMode}
            isDisabled={isSwitchingTheme}
            onChange={handleToggleDarkMode}
        />
    );
}
