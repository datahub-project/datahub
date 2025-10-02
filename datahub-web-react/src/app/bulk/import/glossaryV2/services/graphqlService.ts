/**
 * GraphQL service for DataHub API interactions
 */

import { gql } from '@apollo/client';

// Types for patch operations
export interface PatchOperation {
  op: 'ADD' | 'REMOVE' | 'REPLACE' | 'MOVE' | 'COPY' | 'TEST';
  path: string;
  value?: any;
  from?: string;
}

export interface ArrayPrimaryKeyInput {
  arrayField: string;
  keys: string[];
}

export interface EntityPatchInput {
  urn?: string;
  entityType: string;
  aspectName: string;
  patch: PatchOperation[];
  arrayPrimaryKeys?: ArrayPrimaryKeyInput[];
  forceGenericPatch?: boolean;
}

export class GraphQLService {
  /**
   * Unified GraphQL query for both entity lookup and export with comprehensive fields
   */
  static readonly UNIFIED_GLOSSARY_QUERY = gql`
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

  /**
   * Patch Entities Mutation - Leverages universal patch support
   */
  static readonly PATCH_ENTITIES_MUTATION = gql`
    mutation patchEntities($input: [PatchEntityInput!]!) {
      patchEntities(input: $input) {
        urn
        success
        error
      }
    }
  `;

  /**
   * Get ownership types query
   */
  static readonly GET_OWNERSHIP_TYPES = gql`
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

  /**
   * Execute unified glossary query
   */
  static async executeUnifiedGlossaryQuery(
    apolloClient: any,
    variables: {
      input: {
        types: string[];
        query: string;
        count: number;
      }
    }
  ) {
    try {
      const { data } = await apolloClient.query({
        query: this.UNIFIED_GLOSSARY_QUERY,
        variables,
        fetchPolicy: 'network-only'
      });

      return data?.scrollAcrossEntities?.searchResults || [];
    } catch (error) {
      console.error('Failed to execute unified glossary query:', error);
      throw error;
    }
  }

  /**
   * Execute patch entities mutation
   */
  static async executePatchEntitiesMutation(
    apolloClient: any,
    input: EntityPatchInput[]
  ) {
    try {
      const { data } = await apolloClient.mutate({
        mutation: this.PATCH_ENTITIES_MUTATION,
        variables: { input }
      });

      return data?.patchEntities || [];
    } catch (error) {
      console.error('Failed to execute patch entities mutation:', error);
      throw error;
    }
  }

  /**
   * Execute ownership types query
   */
  static async executeOwnershipTypesQuery(
    apolloClient: any,
    variables: {
      input: {
        start?: number;
        count?: number;
      }
    }
  ) {
    try {
      const { data } = await apolloClient.query({
        query: this.GET_OWNERSHIP_TYPES,
        variables
      });

      return data?.listOwnershipTypes?.ownershipTypes || [];
    } catch (error) {
      console.error('Failed to execute ownership types query:', error);
      throw error;
    }
  }

  /**
   * Build glossaryTermInfo patch operations from CSV row data
   */
  static buildGlossaryTermInfoPatch(row: any, urn?: string, parentUrns?: string[]): EntityPatchInput {
    const patches: PatchOperation[] = [];
    
    // For new entities (no URN), mimic existing mutation behavior
    if (!urn) {
      patches.push({ op: 'ADD', path: '/name', value: row.name || null });
      patches.push({ op: 'ADD', path: '/definition', value: (row.definition || row.description) || "" });
      patches.push({ op: 'ADD', path: '/termSource', value: row.term_source || 'INTERNAL' });
    } else {
      if (row.name) patches.push({ op: 'ADD', path: '/name', value: row.name });
      if (row.definition || row.description) {
        patches.push({ op: 'ADD', path: '/definition', value: row.definition || row.description });
      }
      if (row.term_source) patches.push({ op: 'ADD', path: '/termSource', value: row.term_source });
    }
    
    // Optional fields
    if (row.source_ref) {
      patches.push({ op: 'ADD', path: '/sourceRef', value: row.source_ref });
    }
    if (row.source_url) {
      patches.push({ op: 'ADD', path: '/sourceUrl', value: row.source_url });
    }
    
    // Custom properties
    if (row.custom_properties) {
      try {
        const customProps = typeof row.custom_properties === 'string' 
          ? JSON.parse(row.custom_properties)
          : row.custom_properties;
        
        Object.entries(customProps).forEach(([key, value]) => {
          patches.push({ 
            op: 'ADD', 
            path: `/customProperties/${key}`,
            value: JSON.stringify(String(value))
          });
        });
      } catch (e) {
        console.warn(`Failed to parse custom properties for ${row.name}:`, e);
      }
    }
    
    // Add parent-child relationships
    if (parentUrns && parentUrns.length > 0) {
      const immediateParentUrn = parentUrns[parentUrns.length - 1];
      patches.push({
        op: 'ADD',
        path: '/parentNode',
        value: immediateParentUrn
      });
    }
    
    return {
      urn,
      entityType: 'glossaryTerm',
      aspectName: 'glossaryTermInfo',
      patch: patches
    };
  }

  /**
   * Build glossaryNodeInfo patch operations from CSV row data
   */
  static buildGlossaryNodeInfoPatch(row: any, urn?: string, parentUrns?: string[]): EntityPatchInput {
    const patches: PatchOperation[] = [];
    
    // For new entities (no URN), mimic existing mutation behavior
    if (!urn) {
      patches.push({ op: 'ADD', path: '/name', value: row.name || null });
      patches.push({ op: 'ADD', path: '/definition', value: (row.description || row.definition) || "" });
    } else {
      if (row.name) patches.push({ op: 'ADD', path: '/name', value: row.name });
      if (row.description || row.definition) {
        patches.push({ op: 'ADD', path: '/definition', value: row.description || row.definition });
      }
    }
    
    // Custom properties
    if (row.custom_properties) {
      try {
        const customProps = typeof row.custom_properties === 'string' 
          ? JSON.parse(row.custom_properties)
          : row.custom_properties;
        
        Object.entries(customProps).forEach(([key, value]) => {
          patches.push({ 
            op: 'ADD', 
            path: `/customProperties/${key}`,
            value: JSON.stringify(String(value))
          });
        });
      } catch (e) {
        console.warn(`Failed to parse custom properties for ${row.name}:`, e);
      }
    }
    
    // Add parent-child relationships
    if (parentUrns && parentUrns.length > 0) {
      const immediateParentUrn = parentUrns[parentUrns.length - 1];
      patches.push({
        op: 'ADD',
        path: '/parentNode',
        value: immediateParentUrn
      });
    }
    
    return {
      urn,
      entityType: 'glossaryNode', 
      aspectName: 'glossaryNodeInfo',
      patch: patches
    };
  }

  /**
   * Build basic entity patches from unified entity (only basic properties and parent relationships)
   */
  static buildEntityPatchesFromUnifiedEntity(entity: any): EntityPatchInput[] {
    const patches: EntityPatchInput[] = [];
    
    // Primary aspect based on entity type (only basic properties and parent-child relationships)
    if (entity.type === 'glossaryTerm') {
      patches.push(this.buildGlossaryTermInfoPatch(entity.data, entity.urn, entity.parentUrns));
    } else if (entity.type === 'glossaryNode') {
      patches.push(this.buildGlossaryNodeInfoPatch(entity.data, entity.urn, entity.parentUrns));
    } else {
      throw new Error(`Unsupported entity type: ${entity.type}`);
    }
    
    return patches;
  }

  /**
   * Build domain patches for GlossaryTerm entities (to be run after entity creation)
   */
  static buildDomainPatches(entity: any): EntityPatchInput[] {
    const patches: EntityPatchInput[] = [];
    
    if (entity.type === 'glossaryTerm' && entity.data.domain_urn) {
      const domainPatch = this.buildDomainPatch(entity.data, entity.urn);
      if (domainPatch) {
        patches.push(domainPatch);
      }
    }
    
    return patches;
  }

  /**
   * Build ownership patches for both entity types (to be run after entity creation)
   */
  static buildOwnershipPatches(entity: any): EntityPatchInput[] {
    const patches: EntityPatchInput[] = [];
    
    if (entity.data.ownership) {
      patches.push(this.buildOwnershipPatch(entity.data, entity.type, entity.urn));
    }
    
    return patches;
  }

  /**
   * Build domain patch operations for GlossaryTerm entities
   * Note: GlossaryTerm entities don't have a domains aspect, so we return null
   * Domain assignment should be handled via setDomain mutation instead
   */
  static buildDomainPatch(row: any, urn?: string): EntityPatchInput | null {
    // GlossaryTerm entities don't support domain patches via the domains aspect
    // Domain assignment should be handled via setDomain mutation
    return null;
  }

  /**
   * Build ownership patch operations for both entity types
   */
  static buildOwnershipPatch(row: any, entityType: string, urn?: string): EntityPatchInput {
    const patches: PatchOperation[] = [];
    
    if (row.ownership) {
      try {
        const ownershipEntries = this.parseOwnershipString(row.ownership);
        ownershipEntries.forEach((entry) => {
          patches.push({
            op: 'ADD',
            path: `/owners/${entry.owner}/${entry.type}`,
            value: JSON.stringify({
              owner: entry.owner,
              type: entry.type
            })
          });
        });
      } catch (e) {
        console.warn(`Failed to parse ownership for ${row.name}:`, e);
      }
    }
    
    return {
      urn,
      entityType,
      aspectName: 'ownership',
      patch: patches
    };
  }

  /**
   * Parse ownership string into structured format
   */
  static parseOwnershipString(ownershipStr: string): Array<{owner: string, type: string, ownerType: string}> {
    if (!ownershipStr || !ownershipStr.trim()) return [];
    
    return ownershipStr.split(',').map(item => {
      const trimmed = item.trim();
      if (trimmed.includes(':')) {
        const parts = trimmed.split(':').map(s => s.trim());
        if (parts.length >= 3) {
          // Format: owner:type:ownerType
          const [owner, type, ownerType] = parts;
          return { 
            owner: this.formatUserUrn(owner), 
            type, 
            ownerType 
          };
        } else if (parts.length === 2) {
          // Format: owner:type (assume CORP_USER)
          const [owner, type] = parts;
          return { 
            owner: this.formatUserUrn(owner), 
            type, 
            ownerType: 'CORP_USER' 
          };
        }
      }
      // Fallback: treat as owner with default type
      return { 
        owner: this.formatUserUrn(trimmed), 
        type: 'DATAOWNER', 
        ownerType: 'CORP_USER' 
      };
    });
  }

  /**
   * Format user identifier as proper URN
   */
  static formatUserUrn(userInput: string): string {
    if (!userInput) return userInput;
    
    // If already a URN, return as-is
    if (userInput.startsWith('urn:li:corpuser:')) {
      return userInput;
    }
    
    // If it's just a username, add the URN prefix
    return `urn:li:corpuser:${userInput}`;
  }

  /**
   * Utility function to chunk array into batches
   */
  static chunkArray<T>(array: T[], chunkSize: number): T[][] {
    const chunks: T[][] = [];
    for (let i = 0; i < array.length; i += chunkSize) {
      chunks.push(array.slice(i, i + chunkSize));
    }
    return chunks;
  }
}
