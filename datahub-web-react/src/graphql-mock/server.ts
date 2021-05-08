import { Model, Response, createServer, belongsTo } from 'miragejs';
import { createGraphQLHandler } from '@miragejs/graphql';

import { graphQLSchema } from './schema';
import { GlobalCfg } from '../conf';

import { resolveRequest } from './resolver';
import { createLoginUsers } from './fixtures/user';
import { findUserByURN } from './fixtures/searchResult/userSearchResult';

export function makeServer() {
    // console.log(graphQLSchema);
    return createServer({
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

            this.post('/logIn', (_, request) => {
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
