import './storybook-theme.css';
// FYI: import of antd styles required to show components based on it correctly
import 'antd/dist/antd.css';

import { BADGE, defaultBadgesConfig } from '@geometricpanda/storybook-addon-badges';
import DocTemplate from './DocTemplate.mdx';

const preview = {
    tags: ['!dev', 'autodocs'],
    parameters: {
        previewTabs: {
            'storybook/docs/panel': { index: -1 },
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
                    'Other',
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
        },

        // Reconfig the premade badges with better titles
        badgesConfig: {
            stable: {
                ...defaultBadgesConfig[BADGE.STABLE],
                title: 'Stable',
                tooltip: 'This component is stable but may have frequent changes. Use at own discretion.',
            },
            productionReady: {
                ...defaultBadgesConfig[BADGE.STABLE],
                title: 'Production Ready',
                tooltip: 'This component is production ready and has been tested in a production environment.',
            },
            WIP: {
                ...defaultBadgesConfig[BADGE.BETA],
                title: 'WIP',
                tooltip: 'This component is a work in progress and may not be fully functional or tested.',
            },
            readyForDesignReview: {
                ...defaultBadgesConfig[BADGE.NEEDS_REVISION],
                title: 'Ready for Design Review',
                tooltip: 'This component is ready for design review and feedback.',
            },
        },
    },
};

export default preview;
