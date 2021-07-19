import { loader } from 'graphql.macro';

import gql from 'graphql-tag';
import { buildASTSchema } from 'graphql';

const gmsSchema = loader('../../../datahub-graphql-core/src/main/resources/gms.graphql');
const feSchema = loader('../../../datahub-frontend/conf/datahub-frontend.graphql');

const graphQLSchemaAST = gql`
    ${gmsSchema}
    ${feSchema}
`;

export const graphQLSchema = buildASTSchema(graphQLSchemaAST);
