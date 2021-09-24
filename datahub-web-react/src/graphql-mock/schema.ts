import { loader } from 'graphql.macro';

import gql from 'graphql-tag';
import { buildASTSchema } from 'graphql';

const gmsSchema = loader('../../../datahub-graphql-core/src/main/resources/gms.graphql');

const graphQLSchemaAST = gql`
    ${gmsSchema}
`;

export const graphQLSchema = buildASTSchema(graphQLSchemaAST);
