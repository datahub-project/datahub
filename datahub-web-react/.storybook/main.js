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
        '../src/alchemy-components/components/**/*.stories.@(js|jsx|mjs|ts|tsx)',
        // App-level compositions (e.g. the entity hover card) that build on alchemy components but
        // depend on GraphQL types, so they can't live under `alchemy-components`.
        '../src/app/**/*.stories.@(js|jsx|mjs|ts|tsx)',
    ],
    addons: [
        '@storybook/addon-onboarding',
        '@storybook/addon-essentials',
        '@storybook/addon-interactions',
        '@storybook/addon-links',
        '@geometricpanda/storybook-addon-badges',
    ],
    // preview-head.html loads Mulish from ../src/fonts. The dev server serves that path from the
    // project, but a static build doesn't, so copy the fonts to the same path in the output.
    // Build only: in dev the static mapping would answer Vite's `/src/fonts/*.ttf?import` module
    // requests with the raw font, and the preview fails to load.
    staticDirs: async (dirs, { configType }) =>
        configType === 'PRODUCTION' ? [...(dirs ?? []), { from: '../src/fonts', to: '/src/fonts' }] : (dirs ?? []),
    typescript: {
        reactDocgen: 'react-docgen-typescript',
    },
};
