import { codecovVitePlugin } from '@codecov/vite-plugin';
import react from '@vitejs/plugin-react-swc';
import * as path from 'path';
import { defineConfig, loadEnv } from 'vite';
import macrosPlugin from 'vite-plugin-babel-macros';
import svgr from 'vite-plugin-svgr';

const injectMeticulous = () => {
    if (!process.env.REACT_APP_METICULOUS_PROJECT_TOKEN) {
        return null;
    }

    return {
        name: 'inject-meticulous',
        transformIndexHtml: {
            transform(html) {
                const scriptTag = `
                    <script
                        data-recording-token=${process.env.REACT_APP_METICULOUS_PROJECT_TOKEN}
                        src="https://snippet.meticulous.ai/v1/meticulous.js">
                    </script>
                `;

                return html.replace('</head>', `${scriptTag}\n</head>`);
            },
        },
    };
};

// Replacing at build time, TODO: use Mustache
const injectDatahubEnv = () => {
    // List of environment variables to inject into index.html
    // Format: template placeholder → environment variable name
    const envVariables = [
        { placeholder: '{{__DATAHUB_BASE_PATH__}}', envVar: 'DATAHUB_BASE_PATH' },
        { placeholder: '{{__DATAHUB_APP_VERSION__}}', envVar: 'DATAHUB_APP_VERSION' },
    ];

    return {
        name: 'inject-env',
        transformIndexHtml: {
            order: 'pre', // Run after Vite generates assets
            handler(html) {
                let transformedHtml = html;

                // Iterate through all environment variables and replace placeholders
                envVariables.forEach(({ placeholder, envVar }) => {
                    // Provide sensible defaults for missing env vars
                    const value = process.env[envVar];
                    transformedHtml = transformedHtml.replaceAll(placeholder, value);
                });

                return transformedHtml;
            },
        },
    };
}


// https://vitejs.dev/config/
export default defineConfig(async ({ mode }) => {
    // Get DataHub version from environment variable set by gradle
    const datahubVersion = process.env.DATAHUB_APP_VERSION || '0.0.0';
    // Make sure the env var is set for the plugin FIXME
    process.env.DATAHUB_APP_VERSION = datahubVersion;

    const { viteStaticCopy } = await import('vite-plugin-static-copy');

    // Via https://stackoverflow.com/a/66389044.
    const env = loadEnv(mode, process.cwd(), '');
    process.env = { ...process.env, ...env };

    const themeConfigFile = `./src/conf/theme/${process.env.REACT_APP_THEME_CONFIG}`;
    // eslint-disable-next-line global-require, import/no-dynamic-require, @typescript-eslint/no-var-requires
    const themeConfig = require(themeConfigFile);

    // Setup proxy to the datahub-frontend service.
    const frontendProxyTarget = process.env.REACT_APP_PROXY_TARGET || 'http://localhost:9002';

    const frontendProxy = {
        target: frontendProxyTarget,
        changeOrigin: true,
        // No path rewriting - let the backend handle base path routing
    };

    // Standard API endpoints that need proxying
    const apiEndpoints = [
        '/logIn',
        '/signUp',
        '/resetNativeUserCredentials',
        '/authenticate',
        '/sso',
        '/logOut',
        '/api/v2/graphql',
        '/openapi/v1/tracking/track',
        '/config',  // Add config endpoint for base path detection
    ];

    const proxyOptions = {};

    // Add proxy rules for each endpoint
    apiEndpoints.forEach(endpoint => {
        proxyOptions[endpoint] = frontendProxy;
    });

    const devPlugins = mode === 'development' ? [injectMeticulous()] : [];
    const envs = [injectDatahubEnv()];

    return {
        appType: 'spa',
        base: '/',  // Always use root - runtime base path detection handles deployment paths
        plugins: [
            ...devPlugins,
            ...envs,
            react(),
            svgr(),
            macrosPlugin(),
            viteStaticCopy({
                targets: [
                    // Self-host images by copying them to the build directory
                    { src: path.resolve(__dirname, 'src/images/*'), dest: 'assets/platforms' },
                    // Also keep the theme json files in the build directory
                    { src: path.resolve(__dirname, 'src/conf/theme/*.json'), dest: 'assets/conf/theme' },
                ],
            }),
            viteStaticCopy({
                targets: [
                    // Copy monaco-editor files to the build directory
                    // Because of the structured option, specifying dest .
                    // means that it will mirror the node_modules/... structure
                    // in the build directory.
                    {
                        src: 'node_modules/monaco-editor/min/vs/',
                        dest: '.',
                    },
                    {
                        src: 'node_modules/monaco-editor/min-maps/vs/',
                        dest: '.',
                        rename: (name, ext, fullPath) => {
                            console.log(name, ext, fullPath);
                            return name;
                        },
                    },
                ],
                structured: true,
            }),
            codecovVitePlugin({
                enableBundleAnalysis: true,
                bundleName: 'datahub-react-web',
                uploadToken: process.env.CODECOV_TOKEN,
                gitService: 'github',
            }),
        ],
        // optimizeDeps: {
        //     include: ['@ant-design/colors', '@ant-design/icons', 'lodash-es', '@ant-design/icons/es/icons'],
        // },
        envPrefix: 'REACT_APP_',
        build: {
            outDir: 'dist',
            target: 'esnext',
            minify: 'esbuild',
            reportCompressedSize: false,
            // Limit number of worker threads to reduce CPU pressure
            workers: 3, // default is number of CPU cores
            cssCodeSplit: 'true',
            rollupOptions: {
                output: {
                    assetFileNames: (assetInfo) => {
                        if (/\.(css)$/.test(assetInfo.name)) {
                            return `assets/v${datahubVersion}/css/[name].[ext]`;
                        }
                        if (/\.(png|jpe?g|svg|gif|webp|ico)$/.test(assetInfo.name)) {
                            return `assets/v${datahubVersion}/img/[name].[ext]`;
                        }
                        return `assets/v${datahubVersion}/[name].[ext]`;
                    },
                    chunkFileNames: `assets/v${datahubVersion}/js/[name].js`,
                    entryFileNames: `assets/v${datahubVersion}/js/[name].js`,
                },
            },
        },
        server: {
            open: false,
            host: false,
            port: 3000,
            proxy: proxyOptions,
        },
        css: {
            preprocessorOptions: {
                less: {
                    javascriptEnabled: true,
                    // Override antd theme variables.
                    // https://4x.ant.design/docs/react/customize-theme#Ant-Design-Less-variables
                    modifyVars: themeConfig.styles,
                },
            },
        },
        test: {
            globals: true,
            environment: 'jsdom',
            setupFiles: './src/setupTests.ts',
            css: true,
            // reporters: ['verbose'],
            coverage: {
                enabled: true,
                provider: 'v8',
                reporter: ['text', 'json', 'html'],
                include: ['src/**/*.ts'],
                reportsDirectory: '../build/coverage-reports/datahub-web-react/',
                exclude: [],
            },
        },
        resolve: {
            alias: {
                // Root Directories
                '@src': path.resolve(__dirname, '/src'),
                '@app': path.resolve(__dirname, '/src/app'),
                '@conf': path.resolve(__dirname, '/src/conf'),
                '@components': path.resolve(__dirname, 'src/alchemy-components'),
                '@graphql': path.resolve(__dirname, 'src/graphql'),
                '@graphql-mock': path.resolve(__dirname, 'src/graphql-mock'),
                '@images': path.resolve(__dirname, 'src/images'),
                '@providers': path.resolve(__dirname, 'src/providers'),
                '@utils': path.resolve(__dirname, 'src/utils'),

                // Specific Files
                '@types': path.resolve(__dirname, 'src/types.generated.ts'),
            },
        },
    };
});
