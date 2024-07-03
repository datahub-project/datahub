import './storybook-theme.css';

import DocTemplate from './DocTemplate.mdx';

const preview = {
	tags: ['!dev', 'autodocs'],
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
					// Order of Docs Pages
					'Introduction',
					'Style Guide',
					'Design Tokens',
					'Style Utilities',
					'Icons',

					// Order of Components
					'Layout',
					'Forms',
					'Data Display',
					'Feedback',
					'Typography',
					'Overlay',
					'Disclosure',
					'Navigation',
					'Media',
					'Other'
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
					format: true,
				},
			},
		}
	},
};

export default preview;
