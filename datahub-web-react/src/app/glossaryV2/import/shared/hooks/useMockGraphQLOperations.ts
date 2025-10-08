import { useCallback } from 'react';
import { UseGraphQLOperationsReturn } from './useGraphQLOperations';
import { mockEntities, mockExistingEntities } from '../mocks/mockData';

export function useMockGraphQLOperations(): UseGraphQLOperationsReturn {
  const executeUnifiedGlossaryQuery = useCallback(async (variables: any) => {
    // Mock GraphQL query result
    return {
      data: {
        searchAcrossEntities: {
          searchResults: mockExistingEntities.map(entity => ({
            entity: {
              __typename: entity.type === 'glossaryTerm' ? 'GlossaryTerm' : 'GlossaryNode',
              urn: entity.urn,
              name: entity.name,
              hierarchicalName: entity.name,
              properties: {
                __typename: entity.type === 'glossaryTerm' ? 'GlossaryTermProperties' : 'GlossaryNodeProperties',
                name: entity.name,
                description: entity.data.description,
                termSource: entity.data.term_source,
                sourceRef: entity.data.source_ref,
                sourceUrl: entity.data.source_url,
                customProperties: entity.data.custom_properties ? 
                  entity.data.custom_properties.split(',').map(cp => {
                    const [key, value] = cp.split(':');
                    return { key, value };
                  }) : null
              },
              ownership: {
                __typename: 'Ownership',
                owners: [
                  {
                    __typename: 'Owner',
                    owner: {
                      __typename: 'CorpUser',
                      urn: 'urn:li:corpuser:admin',
                      username: 'admin',
                      info: {
                        __typename: 'CorpUserInfo',
                        displayName: 'Admin User',
                        email: 'admin@example.com',
                        firstName: 'Admin',
                        lastName: 'User',
                        fullName: 'Admin User'
                      }
                    },
                    type: 'TECHNICAL_OWNER',
                    ownershipType: {
                      __typename: 'OwnershipTypeEntity',
                      urn: 'urn:li:ownershipType:developer',
                      info: {
                        __typename: 'OwnershipTypeInfo',
                        name: 'DEVELOPER',
                        description: 'Technical Developer'
                      }
                    }
                  }
                ]
              },
              parentNodes: {
                __typename: 'ParentNodesResult',
                count: entity.parentNames.length,
                nodes: entity.parentNames.map(parentName => ({
                  __typename: 'GlossaryNode',
                  urn: `urn:li:glossaryNode:${parentName.toLowerCase().replace(/\s+/g, '-')}`,
                  name: parentName,
                  properties: {
                    __typename: 'GlossaryNodeProperties',
                    name: parentName,
                    description: null,
                    customProperties: null
                  }
                }))
              },
              domain: null
            }
          }))
        }
      }
    };
  }, []);

  const executePatchEntitiesMutation = useCallback(async (patchInputs: any[]) => {
    // Mock patch mutation result
    return {
      data: {
        patchEntities: patchInputs.map((input, index) => ({
          urn: input.urn || `urn:li:${input.entityType}:mock-${index}`,
          name: input.patch.find((p: any) => p.path === '/name')?.value?.replace(/"/g, '') || 'Mock Entity',
          success: true,
          error: null
        }))
      }
    };
  }, []);

  const executeAddRelatedTermsMutation = useCallback(async (variables: any) => {
    // Mock related terms mutation
    return {
      data: {
        addRelatedTerms: {
          success: true,
          error: null
        }
      }
    };
  }, []);

  const executeSetDomainMutation = useCallback(async (variables: any) => {
    // Mock domain mutation
    return {
      data: {
        setDomain: {
          success: true,
          error: null
        }
      }
    };
  }, []);

  const executeBatchSetDomainMutation = useCallback(async (variables: any) => {
    // Mock batch domain mutation
    return {
      data: {
        batchSetDomain: {
          success: true,
          error: null
        }
      }
    };
  }, []);

  const executeGetOwnershipTypesQuery = useCallback(async () => {
    // Mock ownership types query
    return {
      data: {
        listOwnershipTypes: {
          ownershipTypes: [
            {
              urn: 'urn:li:ownershipType:developer',
              info: {
                name: 'DEVELOPER',
                description: 'Technical Developer'
              }
            },
            {
              urn: 'urn:li:ownershipType:technical-owner',
              info: {
                name: 'Technical Owner',
                description: 'Technical Owner'
              }
            },
            {
              urn: 'urn:li:ownershipType:business-owner',
              info: {
                name: 'Business Owner',
                description: 'Business Owner'
              }
            }
          ]
        }
      }
    };
  }, []);

  return {
    executeUnifiedGlossaryQuery,
    executePatchEntitiesMutation,
    executeAddRelatedTermsMutation,
    executeSetDomainMutation,
    executeBatchSetDomainMutation,
    executeGetOwnershipTypesQuery
  };
}
