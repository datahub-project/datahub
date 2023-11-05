import * as path from 'path';
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import svgr from 'vite-plugin-svgr';
import macrosPlugin from 'vite-plugin-babel-macros';
import { viteStaticCopy } from 'vite-plugin-static-copy';

let proxyOptions = {};
const mockServer = process.env.REACT_APP_MOCK;
if (mockServer !== 'true' && mockServer !== 'cy') {
    const frontendProxy = {
        target: 'http://localhost:9002',
        changeOrigin: true,
    };

    proxyOptions = {
        '/logIn': frontendProxy,
        '/authenticate': frontendProxy,
        '/api/v2/graphql': frontendProxy,
        '/track': frontendProxy,
    };
}

export default defineConfig({
    plugins: [
        react(),
        svgr(),
        macrosPlugin(),
        viteStaticCopy({
            targets: [
                // Self-host images by copying them to the build directory
                { src: path.resolve(__dirname, 'src/images'), dest: 'assets/assets/platforms' },
                // Copy monaco-editor files to the build directory
                // { src: path.resolve(__dirname, 'node_modules/monaco-editor/min/vs/'), dest: 'monaco-editor/vs' },
                // {
                //     src: path.resolve(__dirname, 'node_modules/monaco-editor/min-maps/vs/'),
                //     dest: 'monaco-editor/min-maps/vs',
                // },
            ],
        }),
    ],
    optimizeDeps: {
        include: ['@ant-design/colors', '@ant-design/icons', 'lodash-es', '@ant-design/icons/es/icons'],
    },
    envPrefix: 'REACT_APP_',
    build: {
        outDir: 'build/yarn',
    },
    server: {
        open: false,
        port: 3000,
        proxy: proxyOptions,
    },
    css: {
        preprocessorOptions: {
            less: {
                javascriptEnabled: true,
                additionalData: '@root-entry-name: default;',
            },
        },
    },
});
