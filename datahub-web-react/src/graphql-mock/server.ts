/* eslint-disable no-restricted-syntax */
import { Model, Response, createServer, belongsTo } from 'miragejs';
import { createGraphQLHandler } from '@miragejs/graphql';

import { graphQLSchema } from './schema';
import { GlobalCfg } from '../conf';

import { resolveRequest } from './resolver';
import { createLoginUsers } from './fixtures/user';
import { findUserByURN } from './fixtures/searchResult/userSearchResult';

export function makeServer(environment = 'development') {
    return createServer({
        environment,
        models: {
            CorpUser: Model.extend({
                info: belongsTo('CorpUserInfo'),
                editableInfo: belongsTo('CorpUserEditableInfo'),
            }),
        },

        seeds(server) {
            createLoginUsers(server);

            console.log(server.db.dump());
        },

        routes() {
            const graphQLHandler = createGraphQLHandler(graphQLSchema, this.schema);

            this.post('/api/v2/graphql', (schema, request) => {
                return resolveRequest(schema, request) ?? graphQLHandler(schema, request);
            });

            this.get('/authenticate', () => new Response(200));

            this.post('/logIn', (_schema, request) => {
                const payload = JSON.parse(request.requestBody);
                const cookieExpiration = new Date(Date.now() + 24 * 3600 * 1000);
                const urn = `urn:li:corpuser:${payload.username}`;
                const user = findUserByURN(urn);

                if (!user) {
                    return new Response(404);
                }

                document.cookie = `${
                    GlobalCfg.CLIENT_AUTH_COOKIE
                }=${urn}; domain=localhost; path=/; expires=${cookieExpiration.toUTCString()};`;

                return new Response(200);
            });

            this.post('/track', () => new Response(200));
        },
    });
}

export function makeServerForCypress() {
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    if (window.Cypress) {
        const otherDomains = [];
        const methods = ['head', 'get', 'put', 'patch', 'post', 'delete'];

        createServer({
            environment: 'test',

            routes() {
                for (const domain of ['/*', ...otherDomains]) {
                    for (const method of methods) {
                        this[method](`${domain}`, async (_schema, request) => {
                            // eslint-disable-next-line @typescript-eslint/ban-ts-comment
                            // @ts-ignore
                            const [status, headers, body] = await window.handleFromCypress(request);
                            return new Response(status, headers, body);
                        });
                    }
                }
            },
        });
    }
}
