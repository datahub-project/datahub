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