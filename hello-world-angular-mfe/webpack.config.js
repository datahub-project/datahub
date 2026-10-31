const path = require('path');
const HtmlWebpackPlugin = require('html-webpack-plugin');
const MiniCssExtractPlugin = require('mini-css-extract-plugin');
const { ModuleFederationPlugin } = require('webpack').container;

/**
 * Standalone Webpack configuration for Angular MFE with Module Federation
 * 
 * This config bypasses Angular CLI to properly support Module Federation
 * with Vite-based hosts that expect 'var' format.
 */
module.exports = (env, argv) => {
    const isProduction = argv.mode === 'production';
    const publicPath = isProduction
        ? (process.env.MFE_PUBLIC_PATH || 'auto')
        : 'http://localhost:3004/';

    return {
        entry: './src/main.ts',
        mode: isProduction ? 'production' : 'development',
        devtool: isProduction ? 'source-map' : 'eval-cheap-module-source-map',

        devServer: {
            port: 3004,
            hot: false, // HMR doesn't work well with Module Federation
            liveReload: true,
            headers: {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, PATCH, OPTIONS',
                'Access-Control-Allow-Headers': 'X-Requested-With, content-type, Authorization',
            },
            historyApiFallback: true,
            static: {
                directory: path.join(__dirname, 'dist'),
            },
        },

        output: {
            publicPath,
            path: path.resolve(__dirname, 'dist'),
            filename: isProduction ? '[name].[contenthash].js' : '[name].js',
            clean: true,
        },

        resolve: {
            extensions: ['.ts', '.js'],
            mainFields: ['browser', 'module', 'main'],
        },

        module: {
            rules: [
                {
                    test: /\.ts$/,
                    use: [
                        {
                            loader: 'ts-loader',
                            options: {
                                transpileOnly: true,
                                configFile: 'tsconfig.json',
                            },
                        },
                    ],
                    exclude: /node_modules/,
                },
                {
                    test: /\.css$/,
                    use: [
                        isProduction ? MiniCssExtractPlugin.loader : 'style-loader',
                        'css-loader',
                    ],
                },
            ],
        },

        plugins: [
            new ModuleFederationPlugin({
                name: 'helloWorldAngularMFE',
                library: { type: 'var', name: 'helloWorldAngularMFE' },
                filename: 'remoteEntry.js',
                exposes: {
                    './mount': './src/mount.ts',
                },
                shared: {},
            }),

            new HtmlWebpackPlugin({
                template: './src/index.html',
            }),

            ...(isProduction ? [new MiniCssExtractPlugin()] : []),
        ],

        optimization: {
            splitChunks: false,
            runtimeChunk: false,
        },

        performance: {
            hints: isProduction ? 'warning' : false,
            maxEntrypointSize: 1024000,
            maxAssetSize: 1024000,
        },
    };
};
