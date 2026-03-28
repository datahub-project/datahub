const path = require('path');
const HtmlWebpackPlugin = require('html-webpack-plugin');
const { ModuleFederationPlugin } = require('webpack').container;

/**
 * Webpack configuration for Hello World MFE
 * 
 * This config supports both local development and production builds.
 * 
 * Environment variables:
 *   - MFE_PUBLIC_PATH: The public URL where the MFE will be hosted (for production)
 *                       Example: "https://your-cdn.com/hello-world-mfe/"
 * 
 * Usage:
 *   - Development: npm start (runs on localhost:3002)
 *   - Production:  MFE_PUBLIC_PATH=https://your-cdn.com/mfe/ npm run build
 */
module.exports = (env, argv) => {
    const isProduction = argv.mode === 'production';
    
    // For production, use MFE_PUBLIC_PATH env var or default to auto
    const publicPath = isProduction 
        ? (process.env.MFE_PUBLIC_PATH || 'auto')
        : 'http://localhost:3002/';

    return {
        entry: './src/index.tsx',
        mode: isProduction ? 'production' : 'development',
        
        devtool: isProduction ? 'source-map' : 'eval-cheap-module-source-map',
        
        devServer: {
            port: 3002,
            hot: true,
            headers: {
                // CORS headers for local development
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, PATCH, OPTIONS',
                'Access-Control-Allow-Headers': 'X-Requested-With, content-type, Authorization',
            },
            historyApiFallback: true,
        },
        
        output: {
            publicPath,
            path: path.resolve(__dirname, 'dist'),
            filename: isProduction ? '[name].[contenthash].js' : '[name].js',
            clean: true,
        },
        
        resolve: {
            extensions: ['.tsx', '.ts', '.js', '.jsx'],
        },
        
        module: {
            rules: [
                {
                    test: /\.(ts|tsx)$/,
                    exclude: /node_modules/,
                    use: {
                        loader: 'ts-loader',
                        options: {
                            transpileOnly: true, // Faster builds
                        },
                    },
                },
                {
                    test: /\.css$/,
                    use: ['style-loader', 'css-loader'],
                },
                {
                    test: /\.(png|svg|jpg|jpeg|gif|ico)$/i,
                    type: 'asset/resource',
                },
            ],
        },
        
        plugins: [
            new ModuleFederationPlugin({
                /**
                 * IMPORTANT: This name must match the module path in your MFE config.
                 * If your config has: module: 'helloWorldMFE/mount'
                 * Then this name must be: 'helloWorldMFE'
                 */
                name: 'helloWorldMFE',
                
                /**
                 * This is the file DataHub loads to discover your MFE.
                 * Don't change this unless you also update your MFE config.
                 */
                filename: 'remoteEntry.js',
                
                /**
                 * Expose your mount function.
                 * The key './mount' corresponds to the second part of module path.
                 * If your config has: module: 'helloWorldMFE/mount'
                 * Then this key must be: './mount'
                 */
                exposes: {
                    './mount': './src/mount.tsx',
                },
                
                /**
                 * Share React dependencies with DataHub host.
                 * This prevents duplicate React instances and reduces bundle size.
                 */
                shared: {
                    react: {
                        singleton: true,
                        requiredVersion: '^18.0.0',
                        eager: true,
                    },
                    'react-dom': {
                        singleton: true,
                        requiredVersion: '^18.0.0',
                        eager: true,
                    },
                },
            }),
            
            // HTML template for standalone development
            new HtmlWebpackPlugin({
                template: './public/index.html',
            }),
        ],
        
        optimization: {
            // Don't split chunks for Module Federation remote
            splitChunks: false,
        },
        
        // Performance hints
        performance: {
            hints: isProduction ? 'warning' : false,
            maxEntrypointSize: 512000,
            maxAssetSize: 512000,
        },
    };
};
