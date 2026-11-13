import { addons } from '@storybook/manager-api';

import './storybook-theme.css';
import { darkTheme, lightTheme } from './storybook-theme.js';

const DARK_THEME_ID = 'themeV2Dark';

// Event names are used as literals rather than imported from core-events: that
// entry point moved packages between Storybook 8 minors, and the wire names
// have been stable far longer than the import path.
const SET_GLOBALS = 'setGlobals';
const GLOBALS_UPDATED = 'globalsUpdated';

// The manager renders in its own frame, so the preview's ThemeProvider cannot
// reach it. Listening for the toolbar's theme global is what keeps the sidebar
// and toolbar in step with the stories.
const applyTheme = (globals) => {
    addons.setConfig({
        theme: globals?.theme === DARK_THEME_ID ? darkTheme : lightTheme,
    });
};

addons.setConfig({ theme: lightTheme });

addons.register('datahub/theme-sync', () => {
    const channel = addons.getChannel();
    channel.on(SET_GLOBALS, ({ globals }) => applyTheme(globals));
    channel.on(GLOBALS_UPDATED, ({ globals }) => applyTheme(globals));
});

// Favicon
const link = document.createElement('link');
link.setAttribute('rel', 'shortcut icon');
link.setAttribute('href', 'https://www.acryldata.io/icons/favicon.ico');
document.head.appendChild(link);
