import * as path from 'path';
import { defineConfig, loadEnv } from 'vite';
import react from '@vitejs/plugin-react';
import svgr from 'vite-plugin-svgr';
import macrosPlugin from 'vite-plugin-babel-macros';
import { viteStaticCopy } from 'vite-plugin-static-copy';

// https://vitejs.dev/config/
export default defineConfig(({ mode }) => {
    // Via https://stackoverflow.com/a/66389044.
    const env = loadEnv(mode, process.cwd(), '');
    process.env = { ...process.env, ...env };

    // eslint-disable-next-line global-require, import/no-dynamic-require, @typescript-eslint/no-var-requires
    const themeConfig = require(`./src/conf/theme/${process.env.REACT_APP_THEME_CONFIG}`);

    // Setup proxy to the datahub-frontend service.
    const frontendProxy = {
        target: process.env.REACT_APP_PROXY_TARGET || 'http://localhost:9002',
        changeOrigin: true,
    };
    const proxyOptions = {
        '/logIn': frontendProxy,
        '/authenticate': frontendProxy,
        '/api/v2/graphql': frontendProxy,
        '/track': frontendProxy,
    };

    return {
        appType: 'spa',
        plugins: [
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
        ],
        // optimizeDeps: {
        //     include: ['@ant-design/colors', '@ant-design/icons', 'lodash-es', '@ant-design/icons/es/icons'],
        // },
        envPrefix: 'REACT_APP_',
        build: {
            outDir: 'dist',
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
                reporter: ['text', 'json', 'html'],
                include: ['src/**/*'],
                exclude: [],
            },
        },
    };
});
