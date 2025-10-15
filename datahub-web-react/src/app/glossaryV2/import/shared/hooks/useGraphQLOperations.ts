/**
 * Hook for GraphQL operations
 */

import { useCallback } from 'react';
import { gql, useApolloClient } from '@apollo/client';
import { 
  GraphQLEntity, 
  EntityPatchInput, 
  UseGraphQLOperationsReturn 
} from '../../glossary.types';

// GraphQL query for fetching glossary entities (with pagination support)
const UNIFIED_GLOSSARY_QUERY = gql`
  query getUnifiedGlossaryData($input: ScrollAcrossEntitiesInput!) {
    scrollAcrossEntities(input: $input) {
      nextScrollId
      count
      total
      searchResults {
        entity {
          __typename
          ... on GlossaryTerm {
            urn
            name
            hierarchicalName
            properties {
              name
              description
              termSource
              sourceRef
              sourceUrl
              customProperties {
                key
                value
              }
            }
            contains: relationships(input: { types: ["HasA"], direction: OUTGOING, start: 0, count: 1000 }) {
              relationships {
                entity {
                  ... on GlossaryTerm {
                    urn
                    hierarchicalName
                    properties { name }
                    parentNodes {
                      nodes {
                        urn
                        properties { name }
                      }
                    }
                  }
                  ... on GlossaryNode {
                    urn
                    properties { name }
                    parentNodes {
                      nodes {
                        urn
                        properties { name }
                      }
                    }
                  }
                }
              }
            }
            inherits: relationships(input: { types: ["IsA"], direction: OUTGOING, start: 0, count: 1000 }) {
              relationships {
                entity {
                  ... on GlossaryTerm {
                    urn
                    hierarchicalName
                    properties { name }
                    parentNodes {
                      nodes {
                        urn
                        properties { name }
                      }
                    }
                  }
                }
              }
            }
            relatedTerms: relationships(input: { types: ["RelatedTo"], direction: OUTGOING, start: 0, count: 1000 }) {
              relationships {
                entity {
                  ... on GlossaryTerm {
                    urn
                    hierarchicalName
                    properties { name }
                    parentNodes {
                      nodes {
                        urn
                        properties { name }
                      }
                    }
                  }
                }
              }
            }
            ownership {
              owners {
                owner {
                  __typename
                  ... on CorpUser {
                    urn
                    username
                    info {
                      displayName
                      email
                      firstName
                      lastName
                      fullName
                    }
                  }
                  ... on CorpGroup {
                    urn
                    name
                    info {
                      displayName
                      description
                    }
                  }
                }
                type
                ownershipType {
                  urn
                  info {
                    name
                    description
                  }
                }
              }
            }
            parentNodes {
              count
              nodes {
                urn
                properties { name }
              }
            }
            domain {
              domain {
                urn
                properties { name description }
              }
            }
          }
          ... on GlossaryNode {
            urn
            properties {
              name
              description
              customProperties {
                key
                value
              }
            }
            contains: relationships(input: { types: ["HasA"], direction: OUTGOING, start: 0, count: 1000 }) {
              relationships {
                entity {
                  ... on GlossaryTerm {
                    urn
                    hierarchicalName
                    properties { name }
                  }
                  ... on GlossaryNode {
                    urn
                    properties { name }
                  }
                }
              }
            }
            ownership {
              owners {
                owner {
                  __typename
                  ... on CorpUser {
                    urn
                    username
                    info {
                      displayName
                      email
                      firstName
                      lastName
                      fullName
                    }
                  }
                  ... on CorpGroup {
                    urn
                    name
                    info {
                      displayName
                      description
                    }
                  }
                }
                type
                ownershipType {
                  urn
                  info {
                    name
                    description
                  }
                }
              }
            }
            parentNodes {
              count
              nodes {
                urn
                properties { name }
              }
            }
          }
        }
      }
    }
  }
`;

// GraphQL mutation for patching entities
const PATCH_ENTITIES_MUTATION = gql`
  mutation patchEntities($input: [PatchEntityInput!]!) {
    patchEntities(input: $input) {
      urn
      success
      error
    }
  }
`;

// GraphQL query for getting ownership types
const GET_OWNERSHIP_TYPES = gql`
  query getOwnershipTypes($input: ListOwnershipTypesInput!) {
    listOwnershipTypes(input: $input) {
      start
      count
      total
      ownershipTypes {
        urn
        info {
          name
          description
        }
        status {
          removed
        }
      }
    }
  }
`;

// GraphQL mutation for adding related terms
const ADD_RELATED_TERMS_MUTATION = gql`
  mutation addRelatedTerms($input: RelatedTermsInput!) {
    addRelatedTerms(input: $input)
  }
`;


// GraphQL mutation for setting domain
const SET_DOMAIN_MUTATION = gql`
  mutation setDomain($entityUrn: String!, $domainUrn: String!) {
    setDomain(entityUrn: $entityUrn, domainUrn: $domainUrn)
  }
`;

// GraphQL mutation for batch setting domain
const BATCH_SET_DOMAIN_MUTATION = gql`
  mutation batchSetDomain($input: BatchSetDomainInput!) {
    batchSetDomain(input: $input)
  }
`;

export function useGraphQLOperations(): UseGraphQLOperationsReturn {
  const apolloClient = useApolloClient();

  /**
   * Execute unified glossary query with scrolling support for large result sets
   */
  const executeUnifiedGlossaryQuery = useCallback(async (
    variables: {
      input: {
        types: string[];
        query: string;
        count: number;
        scrollId?: string;
      }
    }
  ): Promise<GraphQLEntity[]> => {
    try {
      const allEntities: GraphQLEntity[] = [];
      let scrollId: string | null = variables.input.scrollId || null;
      let hasMore = true;
      
      // Fetch in batches with scrolling to avoid timeout
      // Using smaller batch size (50) due to deep nested relationship data
      const BATCH_SIZE = 50;
      
      while (hasMore) {
        const { data } = await apolloClient.query({
          query: UNIFIED_GLOSSARY_QUERY,
          variables: {
            input: {
              ...variables.input,
              count: Math.min(variables.input.count - allEntities.length, BATCH_SIZE),
              scrollId: scrollId || undefined,
            }
          },
          fetchPolicy: 'network-only',
        });

        const results = data?.scrollAcrossEntities?.searchResults?.map((result: any) => result.entity) || [];
        allEntities.push(...results);
        
        // Check if there are more results
        const total = data?.scrollAcrossEntities?.total || 0;
        scrollId = data?.scrollAcrossEntities?.nextScrollId;
        
        // Continue if we haven't reached the requested count and there are more results available
        hasMore = allEntities.length < Math.min(variables.input.count, total) && scrollId != null;
      }

      return allEntities;
    } catch (error: any) {
      console.error('Failed to execute unified glossary query:', error);
      
      // Provide more detailed error information
      if (error.networkError) {
        throw new Error(`Network error: ${error.networkError.message || 'Failed to connect to server'}. The query may be too complex or the server may be unresponsive.`);
      }
      
      if (error.message?.includes('JSON')) {
        throw new Error('Server returned invalid response. This may be due to a timeout or server error. Try reducing the amount of data being fetched.');
      }
      
      throw error;
    }
  }, [apolloClient]);

  /**
   * Execute patch entities mutation
   */
  const executePatchEntitiesMutation = useCallback(async (
    input: EntityPatchInput[]
  ): Promise<any> => {
    try {
      const { data } = await apolloClient.mutate({
        mutation: PATCH_ENTITIES_MUTATION,
        variables: { input },
      });

      return data?.patchEntities || [];
    } catch (error) {
      console.error('Failed to execute patch entities mutation:', error);
      throw error;
    }
  }, [apolloClient]);

  /**
   * Execute add related terms mutation
   */
  const executeAddRelatedTermsMutation = useCallback(async (
    input: any
  ): Promise<any> => {
    try {
      const { data } = await apolloClient.mutate({
        mutation: ADD_RELATED_TERMS_MUTATION,
        variables: { input },
      });

      return data?.addRelatedTerms;
    } catch (error) {
      console.error('Failed to execute add related terms mutation:', error);
      throw error;
    }
  }, [apolloClient]);


  /**
   * Execute set domain mutation
   */
  const executeSetDomainMutation = useCallback(async (
    entityUrn: string, 
    domainUrn: string
  ): Promise<any> => {
    try {
      const { data } = await apolloClient.mutate({
        mutation: SET_DOMAIN_MUTATION,
        variables: { entityUrn, domainUrn },
      });

      return data?.setDomain;
    } catch (error) {
      console.error('Failed to execute set domain mutation:', error);
      throw error;
    }
  }, [apolloClient]);

  /**
   * Execute batch set domain mutation
   */
  const executeBatchSetDomainMutation = useCallback(async (
    domainUrn: string,
    entityUrns: string[]
  ): Promise<any> => {
    try {
      const { data } = await apolloClient.mutate({
        mutation: BATCH_SET_DOMAIN_MUTATION,
        variables: { 
          input: {
            domainUrn,
            resources: entityUrns.map(urn => ({ resourceUrn: urn }))
          }
        },
      });

      return data?.batchSetDomain;
    } catch (error) {
      console.error('Failed to execute batch set domain mutation:', error);
      throw error;
    }
  }, [apolloClient]);

  /**
   * Execute get ownership types query
   */
  const executeGetOwnershipTypesQuery = useCallback(async (
    variables: {
      input: {
        start: number;
        count: number;
      }
    }
  ): Promise<any[]> => {
    try {
      const { data } = await apolloClient.query({
        query: GET_OWNERSHIP_TYPES,
        variables,
        fetchPolicy: 'network-only',
      });

      return data?.listOwnershipTypes?.ownershipTypes || [];
    } catch (error) {
      console.error('Failed to execute get ownership types query:', error);
      throw error;
    }
  }, [apolloClient]);

  /**
   * Handle GraphQL-specific errors
   */
  const handleGraphQLErrors = useCallback((error: any): string => {
    if (error.graphQLErrors && error.graphQLErrors.length > 0) {
      return error.graphQLErrors.map((err: any) => err.message).join(', ');
    }
    
    if (error.networkError) {
      return `Network error: ${error.networkError.message || 'Failed to connect to server'}`;
    }
    
    if (error.message) {
      return error.message;
    }
    
    return 'An unknown error occurred';
  }, []);

  return {
    executeUnifiedGlossaryQuery,
    executePatchEntitiesMutation,
    executeAddRelatedTermsMutation,
    executeSetDomainMutation,
    executeBatchSetDomainMutation,
    executeGetOwnershipTypesQuery,
    handleGraphQLErrors
  };
}

// Export the GraphQL operations for use in other hooks
export {
  UNIFIED_GLOSSARY_QUERY,
  PATCH_ENTITIES_MUTATION,
  GET_OWNERSHIP_TYPES
};
