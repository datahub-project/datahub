/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

// Docs for badges: https://storybook.js.org/addons/@geometricpanda/storybook-addon-badges

export default {
	framework: '@storybook/react-vite',
	features: {
		buildStoriesJson: true,
	},
	core: {
		disableTelemetry: true,
	},
	stories: [
		'../src/alchemy-components/.docs/*.mdx',
		'../src/alchemy-components/components/**/*.stories.@(js|jsx|mjs|ts|tsx)'
	],
	addons: [
		'@storybook/addon-onboarding',
		'@storybook/addon-essentials',
		'@storybook/addon-interactions',
		'@storybook/addon-links',
		'@geometricpanda/storybook-addon-badges',
	],
	typescript: {
		reactDocgen: 'react-docgen-typescript',
	},
}