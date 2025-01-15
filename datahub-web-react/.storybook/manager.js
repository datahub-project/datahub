import './storybook-theme.css';

import { addons } from '@storybook/manager-api';
import acrylTheme from './storybook-theme.js';

// Theme setup
addons.setConfig({
	theme: acrylTheme,
});

// Favicon
const link = document.createElement('link');
link.setAttribute('rel', 'shortcut icon');
link.setAttribute('href', 'https://www.acryldata.io/icons/favicon.ico');
document.head.appendChild(link);