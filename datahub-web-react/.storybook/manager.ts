import './storybook.css';

import { addons } from '@storybook/manager-api';

import acrylTheme from './themes/acryl';

addons.setConfig({
	theme: acrylTheme,
});

const link = document.createElement('link');
link.setAttribute('rel', 'shortcut icon');
link.setAttribute('href', 'https://www.acryldata.io/icons/favicon.ico');
document.head.appendChild(link);