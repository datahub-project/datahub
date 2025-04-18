import { buildASTSchema } from 'graphql';
import gql from 'graphql-tag';
import { loader } from 'graphql.macro';

const entitySchema = loader('../../../datahub-graphql-core/src/main/resources/entity.graphql');
const searchSchema = loader('../../../datahub-graphql-core/src/main/resources/search.graphql');

const graphQLSchemaAST = gql`
    ${entitySchema}
    ${searchSchema}
`;

export const graphQLSchema = buildASTSchema(graphQLSchemaAST);
