/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
