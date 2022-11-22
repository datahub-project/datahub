import * as path from 'path';
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import svgr from 'vite-plugin-svgr';
import macrosPlugin from 'vite-plugin-babel-macros';
import { viteStaticCopy } from 'vite-plugin-static-copy';
import devProxyPlugin from './devProxyPlugin';

export default defineConfig({
    plugins: [
        react(),
        svgr(),
        macrosPlugin(),
        devProxyPlugin(),
        viteStaticCopy({
            targets: [
                // Self-host images by copying them to the build directory
                { src: path.resolve(__dirname, 'src/images'), dest: 'assets/platforms' },
                // Copy monaco-editor files to the build directory
                // { src: path.resolve(__dirname, 'node_modules/monaco-editor/min/vs/'), dest: 'monaco-editor/vs' },
                // {
                //     src: path.resolve(__dirname, 'node_modules/monaco-editor/min-maps/vs/'),
                //     dest: 'monaco-editor/min-maps/vs',
                // },
            ],
        }),
    ],
    server: {
        open: true,
        port: 3000,
    },
    optimizeDeps: {
        exclude: ['babel-runtime'],
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
