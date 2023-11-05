import { ViteDevServer } from 'vite';
import { createProxyMiddleware } from 'http-proxy-middleware';

const logInFilter = function (pathname, req) {
    return pathname.match('^/logIn') && req.method === 'POST';
};

const devProxyPlugin = () => ({
    name: 'configure-server',
    configureServer: (server: ViteDevServer) => {
        const mockServer = server.config.env['REACT_APP_MOCK'];
        if (mockServer !== 'true' && mockServer !== 'cy') {
            const { createProxyMiddleware } = require('http-proxy-middleware');
            const options = {
                target: 'http://localhost:9002',
                changeOrigin: true,
            };

            server.middlewares.use('/logIn', createProxyMiddleware(logInFilter, options));
            server.middlewares.use('/authenticate', createProxyMiddleware(options));
            server.middlewares.use('/api/v2/graphql', createProxyMiddleware(options));
            server.middlewares.use('/track', createProxyMiddleware(options));
        }
    },
});

export default devProxyPlugin;
