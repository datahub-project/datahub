/* eslint-disable @typescript-eslint/no-var-requires */
require('dotenv').config();
const { whenProd } = require('@craco/craco');
const CracoAntDesignPlugin = require('craco-antd');
const path = require('path');
const CopyWebpackPlugin = require('copy-webpack-plugin');

// eslint-disable-next-line import/no-dynamic-require
const themeConfig = require(`./src/conf/theme/${process.env.REACT_APP_THEME_CONFIG}`);

function addLessPrefixToKeys(styles) {
    const output = {};
    Object.keys(styles).forEach((key) => {
        output[`@${key}`] = styles[key];
    });
    return output;
}

module.exports = {
    webpack: {
        configure: {
            optimization: whenProd(() => ({
                splitChunks: {
                    cacheGroups: {
                        vendor: {
                            test: /[\\/]node_modules[\\/]/,
                            name: 'vendors',
                            chunks: 'all',
                        },
                    },
                },
            })),
            resolve: {
                fallback: {
                    fs: false,
                },
            },
            ignoreWarnings: [
                // Ignore webpack 5's missing source map warnings from node_modules
                {
                    module: /node_modules/,
                    message: /source-map-loader/,
                },
            ],
        },
        plugins: {
            add: [
                // Self host images by copying them to the build directory
                new CopyWebpackPlugin({
                    patterns: [{ from: 'src/images', to: 'platforms' }],
                }),
                // Copy monaco-editor files to the build directory
                new CopyWebpackPlugin({
                    patterns: [
                        { from: 'node_modules/monaco-editor/min/vs/', to: 'monaco-editor/vs' },
                        { from: 'node_modules/monaco-editor/min-maps/vs/', to: 'monaco-editor/min-maps/vs' },
                    ],
                }),
            ],
        },
    },
    plugins: [
        {
            plugin: CracoAntDesignPlugin,
            options: {
                customizeThemeLessPath: path.join(__dirname, 'src/conf/theme/global-variables.less'),
                customizeTheme: addLessPrefixToKeys(themeConfig.styles),
            },
        },
    ],
};
