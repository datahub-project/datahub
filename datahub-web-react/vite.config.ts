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

// since we have base: './' for relative paths, vite will set static assets at "./assets/..."
// with a base path configured we can't find them. We want simple "assets/..."
export function stripDotSlashFromAssets() {
    return {
        name: 'strip-dot-slash',
        transformIndexHtml(html) {
            return html.replace(/src="\.\//g, 'src="').replace(/href="\.\//g, 'href="');
        },
    };
}

// https://vitejs.dev/config/
export default defineConfig(async ({ mode }) => {
    const { viteStaticCopy } = await import('vite-plugin-static-copy');

    // Via https://stackoverflow.com/a/66389044.
    const env = loadEnv(mode, process.cwd(), '');
    process.env = { ...process.env, ...env };

    let antThemeConfig: any;
    if (process.env.ANT_THEME_CONFIG) {
        const themeConfigFile = `./src/conf/theme/${process.env.ANT_THEME_CONFIG}`;
        // eslint-disable-next-line global-require, import/no-dynamic-require, @typescript-eslint/no-var-requires
        antThemeConfig = require(themeConfigFile);
    }

    // Setup proxy to the datahub-frontend service.
    const frontendProxy = {
        target: process.env.REACT_APP_PROXY_TARGET || 'http://localhost:9002',
        changeOrigin: true,
    };
    const proxyOptions = {
        '/logIn': frontendProxy,
        '/authenticate': frontendProxy,
        '/api/v2/graphql': frontendProxy,
        '/openapi/v1/tracking/track': frontendProxy,
        '/openapi/v1/files': frontendProxy,
    };

    const devPlugins = mode === 'development' ? [injectMeticulous()] : [];

    return {
        appType: 'spa',
        base: './', // Always use root - runtime base path detection handles deployment paths
        plugins: [
            ...devPlugins,
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
            stripDotSlashFromAssets(),
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
        },
        server: {
            open: false,
            host: "0.0.0.0",
            port: 3000,
            proxy: proxyOptions,
        },
        css: {
            preprocessorOptions: {
                less: {
                    javascriptEnabled: true,
                    // Override antd theme variables.
                    // https://4x.ant.design/docs/react/customize-theme#Ant-Design-Less-variables
                    modifyVars: antThemeConfig,
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
