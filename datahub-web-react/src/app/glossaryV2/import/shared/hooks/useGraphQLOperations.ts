/**
 * Hook for GraphQL operations
 */

import { useCallback } from 'react';
import { gql } from '@apollo/client';
import { 
  GraphQLEntity, 
  EntityPatchInput, 
  UseGraphQLOperationsReturn 
} from '../../glossary.types';

// GraphQL query for fetching glossary entities
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

export function useGraphQLOperations(): UseGraphQLOperationsReturn {
  /**
   * Execute unified glossary query
   */
  const executeUnifiedGlossaryQuery = useCallback(async (
    variables: {
      input: {
        types: string[];
        query: string;
        count: number;
      }
    }
  ): Promise<GraphQLEntity[]> => {
    // This would use the actual Apollo client
    // For now, return empty array as placeholder
    return [];
  }, []);

  /**
   * Execute patch entities mutation
   */
  const executePatchEntitiesMutation = useCallback(async (
    input: EntityPatchInput[]
  ): Promise<any> => {
    // This would use the actual Apollo client
    // For now, return mock results
    return input.map(() => ({ success: true, error: null }));
  }, []);

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
    handleGraphQLErrors
  };
}

// Export the GraphQL operations for use in other hooks
export {
  UNIFIED_GLOSSARY_QUERY,
  PATCH_ENTITIES_MUTATION,
  GET_OWNERSHIP_TYPES
};
