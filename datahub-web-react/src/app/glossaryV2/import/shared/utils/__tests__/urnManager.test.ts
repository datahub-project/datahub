/**
 * Tests for UrnManager
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { UrnManager, AUTO_GENERATE_ALLOWED_ENTITY_TYPES } from '../urnManager';
import { Entity } from '../../../glossary.types';

// Mock entity data
const mockEntityData = {
  entity_type: 'glossaryTerm' as const,
  urn: '',
  name: 'Test Term',
  description: 'Test Description',
  term_source: 'INTERNAL',
  source_ref: '',
  source_url: '',
  ownership_users: '',
  ownership_groups: '',
  parent_nodes: '',
  related_contains: '',
  related_inherits: '',
  domain_urn: '',
  domain_name: '',
  custom_properties: '',
  status: 'new'
};

describe('UrnManager', () => {
  describe('generateGuid', () => {
    it('should generate a valid UUID format', () => {
      const guid = UrnManager.generateGuid();
      expect(guid).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i);
    });

    it('should generate unique GUIDs', () => {
      const guid1 = UrnManager.generateGuid();
      const guid2 = UrnManager.generateGuid();
      expect(guid1).not.toBe(guid2);
    });
  });

  describe('generateEntityUrn', () => {
    it('should generate URN for glossaryTerm', () => {
      const urn = UrnManager.generateEntityUrn('glossaryTerm');
      expect(urn).toMatch(/^urn:li:glossaryTerm:[0-9a-f-]{36}$/i);
    });

    it('should generate URN for glossaryNode', () => {
      const urn = UrnManager.generateEntityUrn('glossaryNode');
      expect(urn).toMatch(/^urn:li:glossaryNode:[0-9a-f-]{36}$/i);
    });

    it('should throw error for unsupported entity type', () => {
      expect(() => UrnManager.generateEntityUrn('dataset'))
        .toThrow(/Auto-generated URNs are only supported for entity types/);
    });

    it('should support all allowed entity types', () => {
      AUTO_GENERATE_ALLOWED_ENTITY_TYPES.forEach(entityType => {
        expect(() => UrnManager.generateEntityUrn(entityType)).not.toThrow();
      });
    });
  });

  describe('generateOwnershipTypeUrn', () => {
    it('should generate URN with sanitized name', () => {
      expect(UrnManager.generateOwnershipTypeUrn('Technical Owner'))
        .toBe('urn:li:ownershipType:technical-owner');
    });

    it('should handle special characters', () => {
      expect(UrnManager.generateOwnershipTypeUrn('Data Owner (Primary)'))
        .toBe('urn:li:ownershipType:data-owner--primary-');
    });

    it('should convert to lowercase', () => {
      expect(UrnManager.generateOwnershipTypeUrn('DEVELOPER'))
        .toBe('urn:li:ownershipType:developer');
    });
  });

  describe('generateGlossaryUrn', () => {
    it('should generate URN with encoded name for glossaryTerm', () => {
      const urn = UrnManager.generateGlossaryUrn('glossaryTerm', 'Customer Name');
      expect(urn).toBe('urn:li:glossaryTerm:Customer%20Name');
    });

    it('should generate URN with encoded name for glossaryNode', () => {
      const urn = UrnManager.generateGlossaryUrn('glossaryNode', 'Business Terms');
      expect(urn).toBe('urn:li:glossaryNode:Business%20Terms');
    });

    it('should encode special characters', () => {
      const urn = UrnManager.generateGlossaryUrn('glossaryTerm', 'Customer/Name');
      expect(urn).toContain('%2F');
    });
  });

  describe('isValidUrn', () => {
    it('should validate correct URN format', () => {
      expect(UrnManager.isValidUrn('urn:li:glossaryTerm:abc-123')).toBe(true);
      expect(UrnManager.isValidUrn('urn:li:glossaryNode:node-1')).toBe(true);
    });

    it('should reject invalid URN format', () => {
      expect(UrnManager.isValidUrn('invalid-urn')).toBe(false);
      expect(UrnManager.isValidUrn('urn:li:glossaryTerm')).toBe(false);
      expect(UrnManager.isValidUrn('urn:li:')).toBe(false);
    });

    it('should handle empty URN based on allowEmpty flag', () => {
      expect(UrnManager.isValidUrn('', false)).toBe(false);
      expect(UrnManager.isValidUrn('', true)).toBe(true);
    });

    it('should handle URNs with multiple colons in ID', () => {
      expect(UrnManager.isValidUrn('urn:li:dataset:(urn:li:dataPlatform:kafka,topic.name,PROD)'))
        .toBe(true);
    });
  });

  describe('extractEntityTypeFromUrn', () => {
    it('should extract entity type from valid URN', () => {
      expect(UrnManager.extractEntityTypeFromUrn('urn:li:glossaryTerm:abc-123'))
        .toBe('glossaryTerm');
      expect(UrnManager.extractEntityTypeFromUrn('urn:li:glossaryNode:node-1'))
        .toBe('glossaryNode');
    });

    it('should return null for invalid URN', () => {
      expect(UrnManager.extractEntityTypeFromUrn('invalid-urn')).toBeNull();
      expect(UrnManager.extractEntityTypeFromUrn('urn:li:')).toBeNull();
    });
  });

  describe('extractEntityIdFromUrn', () => {
    it('should extract ID from valid URN', () => {
      expect(UrnManager.extractEntityIdFromUrn('urn:li:glossaryTerm:abc-123'))
        .toBe('abc-123');
      expect(UrnManager.extractEntityIdFromUrn('urn:li:glossaryNode:node-1'))
        .toBe('node-1');
    });

    it('should handle complex IDs with colons', () => {
      expect(UrnManager.extractEntityIdFromUrn('urn:li:dataset:(urn:li:dataPlatform:kafka,topic.name,PROD)'))
        .toBe('(urn:li:dataPlatform:kafka,topic.name,PROD)');
    });

    it('should return null for invalid URN', () => {
      expect(UrnManager.extractEntityIdFromUrn('invalid-urn')).toBeNull();
    });
  });

  describe('preGenerateUrns', () => {
    it('should generate URNs for new entities', () => {
      const entities: Entity[] = [
        {
          id: 'entity-1',
          name: 'Entity 1',
          type: 'glossaryTerm',
          status: 'new',
          parentNames: [],
          parentUrns: [],
          level: 0,
          data: mockEntityData
        },
        {
          id: 'entity-2',
          name: 'Entity 2',
          type: 'glossaryNode',
          status: 'new',
          parentNames: [],
          parentUrns: [],
          level: 0,
          data: mockEntityData
        }
      ];

      const urnMap = UrnManager.preGenerateUrns(entities);

      expect(urnMap.size).toBe(2);
      expect(urnMap.get('entity-1')).toMatch(/^urn:li:glossaryTerm:/);
      expect(urnMap.get('entity-2')).toMatch(/^urn:li:glossaryNode:/);
    });

    it('should use existing URNs for existing entities', () => {
      const existingUrn = 'urn:li:glossaryTerm:existing-123';
      const entities: Entity[] = [
        {
          id: 'entity-1',
          name: 'Entity 1',
          type: 'glossaryTerm',
          status: 'existing',
          parentNames: [],
          parentUrns: [],
          level: 0,
          data: mockEntityData,
          urn: existingUrn
        }
      ];

      const urnMap = UrnManager.preGenerateUrns(entities);

      expect(urnMap.size).toBe(1);
      expect(urnMap.get('entity-1')).toBe(existingUrn);
    });
  });

  describe('resolveEntityUrn', () => {
    it('should return existing URN if available', () => {
      const existingUrn = 'urn:li:glossaryTerm:existing-123';
      const entity: Entity = {
        id: 'entity-1',
        name: 'Entity 1',
        type: 'glossaryTerm',
        status: 'existing',
        parentNames: [],
        parentUrns: [],
        level: 0,
        data: mockEntityData,
        urn: existingUrn
      };

      const urnMap = new Map<string, string>();
      const resolvedUrn = UrnManager.resolveEntityUrn(entity, urnMap);

      expect(resolvedUrn).toBe(existingUrn);
    });

    it('should return generated URN from map', () => {
      const generatedUrn = 'urn:li:glossaryTerm:generated-456';
      const entity: Entity = {
        id: 'entity-1',
        name: 'Entity 1',
        type: 'glossaryTerm',
        status: 'new',
        parentNames: [],
        parentUrns: [],
        level: 0,
        data: mockEntityData
      };

      const urnMap = new Map<string, string>([['entity-1', generatedUrn]]);
      const resolvedUrn = UrnManager.resolveEntityUrn(entity, urnMap);

      expect(resolvedUrn).toBe(generatedUrn);
    });

    it('should throw error if no URN available', () => {
      const entity: Entity = {
        id: 'entity-1',
        name: 'Entity 1',
        type: 'glossaryTerm',
        status: 'new',
        parentNames: [],
        parentUrns: [],
        level: 0,
        data: mockEntityData
      };

      const urnMap = new Map<string, string>();

      expect(() => UrnManager.resolveEntityUrn(entity, urnMap))
        .toThrow('No URN available for entity Entity 1 (entity-1)');
    });
  });

  describe('createExistingEntityUrnMap', () => {
    it('should create map with entity ID and name keys', () => {
      const entities: Entity[] = [
        {
          id: 'entity-1',
          name: 'Test Entity',
          type: 'glossaryTerm',
          status: 'existing',
          parentNames: [],
          parentUrns: [],
          level: 0,
          data: mockEntityData,
          urn: 'urn:li:glossaryTerm:test-123'
        }
      ];

      const urnMap = UrnManager.createExistingEntityUrnMap(entities);

      expect(urnMap.size).toBe(2); // ID and lowercase name
      expect(urnMap.get('entity-1')).toBe('urn:li:glossaryTerm:test-123');
      expect(urnMap.get('test entity')).toBe('urn:li:glossaryTerm:test-123');
    });
  });

  describe('findUrnByName', () => {
    it('should find URN by exact name match', () => {
      const entities: Entity[] = [
        {
          id: 'entity-1',
          name: 'Test Entity',
          type: 'glossaryTerm',
          status: 'existing',
          parentNames: [],
          parentUrns: [],
          level: 0,
          data: mockEntityData,
          urn: 'urn:li:glossaryTerm:test-123'
        }
      ];

      const urn = UrnManager.findUrnByName('Test Entity', entities);
      expect(urn).toBe('urn:li:glossaryTerm:test-123');
    });

    it('should find URN by case-insensitive match', () => {
      const entities: Entity[] = [
        {
          id: 'entity-1',
          name: 'Test Entity',
          type: 'glossaryTerm',
          status: 'existing',
          parentNames: [],
          parentUrns: [],
          level: 0,
          data: mockEntityData,
          urn: 'urn:li:glossaryTerm:test-123'
        }
      ];

      const urn = UrnManager.findUrnByName('test entity', entities);
      expect(urn).toBe('urn:li:glossaryTerm:test-123');
    });

    it('should return undefined if not found', () => {
      const entities: Entity[] = [];
      const urn = UrnManager.findUrnByName('Non Existent', entities);
      expect(urn).toBeUndefined();
    });
  });

  describe('supportsAutoGeneratedUrn', () => {
    it('should return true for supported types', () => {
      expect(UrnManager.supportsAutoGeneratedUrn('glossaryTerm')).toBe(true);
      expect(UrnManager.supportsAutoGeneratedUrn('glossaryNode')).toBe(true);
      expect(UrnManager.supportsAutoGeneratedUrn('ownershipType')).toBe(true);
    });

    it('should return false for unsupported types', () => {
      expect(UrnManager.supportsAutoGeneratedUrn('dataset')).toBe(false);
      expect(UrnManager.supportsAutoGeneratedUrn('chart')).toBe(false);
    });
  });

  describe('parseUrn', () => {
    it('should parse valid URN into components', () => {
      const parsed = UrnManager.parseUrn('urn:li:glossaryTerm:abc-123');
      expect(parsed).toEqual({
        scheme: 'urn',
        namespace: 'li',
        entityType: 'glossaryTerm',
        id: 'abc-123'
      });
    });

    it('should handle complex IDs', () => {
      const parsed = UrnManager.parseUrn('urn:li:dataset:(urn:li:dataPlatform:kafka,topic.name,PROD)');
      expect(parsed?.id).toBe('(urn:li:dataPlatform:kafka,topic.name,PROD)');
    });

    it('should return null for invalid URN', () => {
      expect(UrnManager.parseUrn('invalid-urn')).toBeNull();
    });
  });

  describe('buildUrn', () => {
    it('should build URN from components', () => {
      const urn = UrnManager.buildUrn('glossaryTerm', 'abc-123');
      expect(urn).toBe('urn:li:glossaryTerm:abc-123');
    });

    it('should handle complex IDs', () => {
      const urn = UrnManager.buildUrn('dataset', '(urn:li:dataPlatform:kafka,topic.name,PROD)');
      expect(urn).toBe('urn:li:dataset:(urn:li:dataPlatform:kafka,topic.name,PROD)');
    });
  });

  describe('isDomainUrn', () => {
    it('should return true for domain URNs', () => {
      expect(UrnManager.isDomainUrn('urn:li:domain:marketing')).toBe(true);
    });

    it('should return false for non-domain URNs', () => {
      expect(UrnManager.isDomainUrn('urn:li:glossaryTerm:abc-123')).toBe(false);
    });
  });

  describe('isOwnershipTypeUrn', () => {
    it('should return true for ownership type URNs', () => {
      expect(UrnManager.isOwnershipTypeUrn('urn:li:ownershipType:technical-owner')).toBe(true);
    });

    it('should return false for non-ownership type URNs', () => {
      expect(UrnManager.isOwnershipTypeUrn('urn:li:glossaryTerm:abc-123')).toBe(false);
    });
  });
});

