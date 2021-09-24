import { loader } from 'graphql.macro';

import gql from 'graphql-tag';
import { buildASTSchema } from 'graphql';

const entitySchema = loader('../../../datahub-graphql-core/src/main/resources/entity.graphql');
const searchSchema = loader('../../../datahub-graphql-core/src/main/resources/search.graphql');

const graphQLSchemaAST = gql`
    ${entitySchema}
    ${searchSchema}
`;

export const graphQLSchema = buildASTSchema(graphQLSchemaAST);
