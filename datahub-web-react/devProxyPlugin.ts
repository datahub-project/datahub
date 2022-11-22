import { ViteDevServer } from 'vite';
import { createProxyMiddleware } from 'http-proxy-middleware';

const logInFilter = function (pathname, req) {
    return pathname.match('^/logIn') && req.method === 'POST';
};

const devProxyPlugin = () => ({
    name: 'configure-server',
    configureServer: (server: ViteDevServer) => {
        const mockServer = server.config.env['VITE_MOCK'];
        if (mockServer !== 'true' && mockServer !== 'cy') {
            const { createProxyMiddleware } = require('http-proxy-middleware');

            server.middlewares.use(
                '/logIn',
                createProxyMiddleware(logInFilter, {
                    target: 'http://localhost:9002',
                    changeOrigin: true,
                }),
            );
            server.middlewares.use(
                '/authenticate',
                createProxyMiddleware({
                    target: 'http://localhost:9002',
                    changeOrigin: true,
                }),
            );
            server.middlewares.use(
                '/api/v2/graphql',
                createProxyMiddleware({
                    target: 'http://localhost:9002',
                    changeOrigin: true,
                }),
            );
        }
    },
});

export default devProxyPlugin;
