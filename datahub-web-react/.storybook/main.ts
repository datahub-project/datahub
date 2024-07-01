import type { StorybookConfig } from '@storybook/react-vite';

const config: StorybookConfig = {
	core: {
		builder: "@storybook/builder-vite",
	},
	framework: {
		name: '@storybook/react-vite',
		options: {},
	},
	stories: ['../docs/**/*.mdx', '../docs/**/*.stories.@(js|jsx|mjs|ts|tsx)'],
	addons: [
		'@storybook/preset-create-react-app',
		'@storybook/addon-onboarding',
		'@storybook/addon-links',
		'@storybook/addon-essentials',
		'@chromatic-com/storybook',
		'@storybook/addon-interactions',
	],
	staticDirs: ['../public', '../src/fonts', '../src/images'],
	typescript: {
		reactDocgen: 'react-docgen-typescript',
	},
};

export default config;
