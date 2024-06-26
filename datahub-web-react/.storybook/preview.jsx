import './storybook.css';

import DocTemplate from './DocTemplate.mdx';

const preview = {
	parameters: {
		previewTabs: {
			'storybook/docs/panel': { index: -1 }
		},
		controls: {
			matchers: {
				color: /(background|color)$/i,
				date: /Date$/i,
			},
		},
		options: {
			storySort: {
				method: 'alphabetical',
				order: [
					'Introduction',
					'Style Guide',
					'Design Tokens',
					'Style Utilities',
					'Icons',
					'Components',
				],
				locales: '',
			},
		},
		docs: {
			page: DocTemplate,
			toc: {
				disable: false,
			},
			docs: {
				source: {
					dark: true,
					format: true,
				},
			},
		}
	},
};

export default preview;
