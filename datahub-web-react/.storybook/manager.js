/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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