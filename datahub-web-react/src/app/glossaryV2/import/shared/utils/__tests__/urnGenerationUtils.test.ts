/**
 * Tests for URN Generation Utilities
 */

import { describe, it, expect } from 'vitest';
import {
  generateGuid,
  generateEntityUrn,
  generateOwnershipTypeUrn,
  preGenerateUrns,
  resolveEntityUrn,
  isValidUrn,
  extractEntityTypeFromUrn,
  createExistingEntityUrnMap,
  AUTO_GENERATE_ALLOWED_ENTITY_TYPES
} from '../urnGenerationUtils';
import { Entity } from '../../../glossary.types';

describe('URN Generation Utils', () => {
  describe('generateGuid', () => {
    it('should generate a valid GUID', () => {
      const guid = generateGuid();
      expect(guid).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i);
    });

    it('should generate unique GUIDs', () => {
      const guid1 = generateGuid();
      const guid2 = generateGuid();
      expect(guid1).not.toBe(guid2);
    });
  });

  describe('generateEntityUrn', () => {
    it('should generate URN for allowed entity types', () => {
      const urn = generateEntityUrn('glossaryTerm');
      expect(urn).toMatch(/^urn:li:glossaryTerm:[0-9a-f-]+$/);
    });

    it('should generate URN for glossaryNode', () => {
      const urn = generateEntityUrn('glossaryNode');
      expect(urn).toMatch(/^urn:li:glossaryNode:[0-9a-f-]+$/);
    });

    it('should generate URN for ownershipType', () => {
      const urn = generateEntityUrn('ownershipType');
      expect(urn).toMatch(/^urn:li:ownershipType:[0-9a-f-]+$/);
    });

    it('should throw error for disallowed entity types', () => {
      expect(() => generateEntityUrn('invalidType')).toThrow(
        'Auto-generated URNs are only supported for entity types'
      );
    });
  });

  describe('generateOwnershipTypeUrn', () => {
    it('should generate URN for ownership type', () => {
      const urn = generateOwnershipTypeUrn('Technical Owner');
      expect(urn).toBe('urn:li:ownershipType:technical-owner');
    });

    it('should sanitize special characters', () => {
      const urn = generateOwnershipTypeUrn('Owner/Manager & Lead');
      expect(urn).toBe('urn:li:ownershipType:owner-manager---lead');
    });
  });

  describe('preGenerateUrns', () => {
    it('should generate URNs for new entities only', () => {
      const entities: Entity[] = [
        {
          id: '1',
          name: 'New Entity',
          type: 'glossaryTerm',
          status: 'new',
          parentNames: [],
          parentUrns: [],
          level: 0,
          data: {} as any
        },
        {
          id: '2',
          name: 'Existing Entity',
          type: 'glossaryTerm',
          status: 'existing',
          urn: 'urn:li:glossaryTerm:existing',
          parentNames: [],
          parentUrns: [],
          level: 0,
          data: {} as any
        }
      ];

      const urnMap = preGenerateUrns(entities);
      
      expect(urnMap.size).toBe(2); // Both new and existing entities are in the map
      expect(urnMap.has('1')).toBe(true);
      expect(urnMap.has('2')).toBe(true); // Existing entity is also in the map
      expect(urnMap.get('1')).toMatch(/^urn:li:glossaryTerm:/);
      expect(urnMap.get('2')).toBe('urn:li:glossaryTerm:existing');
    });

    it('should not generate URNs for entities with existing URNs', () => {
      const entities: Entity[] = [
        {
          id: '1',
          name: 'Entity with URN',
          type: 'glossaryTerm',
          status: 'new',
          urn: 'urn:li:glossaryTerm:existing',
          parentNames: [],
          parentUrns: [],
          level: 0,
          data: {} as any
        }
      ];

      const urnMap = preGenerateUrns(entities);
      expect(urnMap.size).toBe(1); // Existing URN is stored in the map
      expect(urnMap.get('1')).toBe('urn:li:glossaryTerm:existing'); // Uses existing URN
    });
  });

  describe('resolveEntityUrn', () => {
    it('should return existing URN if available', () => {
      const entity: Entity = {
        id: '1',
        name: 'Entity',
        type: 'glossaryTerm',
        status: 'existing',
        urn: 'urn:li:glossaryTerm:existing',
        parentNames: [],
        parentUrns: [],
        level: 0,
        data: {} as any
      };

      const urnMap = new Map<string, string>();
      const urn = resolveEntityUrn(entity, urnMap);
      expect(urn).toBe('urn:li:glossaryTerm:existing');
    });

    it('should return generated URN for new entities', () => {
      const entity: Entity = {
        id: '1',
        name: 'New Entity',
        type: 'glossaryTerm',
        status: 'new',
        parentNames: [],
        parentUrns: [],
        level: 0,
        data: {} as any
      };

      const urnMap = new Map<string, string>();
      urnMap.set('1', 'urn:li:glossaryTerm:generated');
      
      const urn = resolveEntityUrn(entity, urnMap);
      expect(urn).toBe('urn:li:glossaryTerm:generated');
    });

    it('should throw error if no URN available', () => {
      const entity: Entity = {
        id: '1',
        name: 'Entity',
        type: 'glossaryTerm',
        status: 'new',
        parentNames: [],
        parentUrns: [],
        level: 0,
        data: {} as any
      };

      const urnMap = new Map<string, string>();
      expect(() => resolveEntityUrn(entity, urnMap)).toThrow('No URN available for entity');
    });
  });

  describe('isValidUrn', () => {
    it('should validate correct URNs', () => {
      expect(isValidUrn('urn:li:glossaryTerm:test')).toBe(true);
      expect(isValidUrn('urn:li:glossaryNode:test')).toBe(true);
      expect(isValidUrn('urn:li:ownershipType:test')).toBe(true);
    });

    it('should reject invalid URNs', () => {
      expect(isValidUrn('')).toBe(false);
      expect(isValidUrn('invalid')).toBe(false);
      expect(isValidUrn('urn:li:')).toBe(false);
      expect(isValidUrn('urn:li:glossaryTerm')).toBe(false);
    });
  });

  describe('extractEntityTypeFromUrn', () => {
    it('should extract entity type from valid URNs', () => {
      expect(extractEntityTypeFromUrn('urn:li:glossaryTerm:test')).toBe('glossaryTerm');
      expect(extractEntityTypeFromUrn('urn:li:glossaryNode:test')).toBe('glossaryNode');
      expect(extractEntityTypeFromUrn('urn:li:ownershipType:test')).toBe('ownershipType');
    });

    it('should return null for invalid URNs', () => {
      expect(extractEntityTypeFromUrn('invalid')).toBe(null);
      expect(extractEntityTypeFromUrn('urn:li:')).toBe(null);
    });
  });

  describe('createExistingEntityUrnMap', () => {
    it('should create URN map from existing entities', () => {
      const entities: Entity[] = [
        {
          id: '1',
          name: 'Entity 1',
          type: 'glossaryTerm',
          status: 'existing',
          urn: 'urn:li:glossaryTerm:entity1',
          parentNames: [],
          parentUrns: [],
          level: 0,
          data: {} as any
        },
        {
          id: '2',
          name: 'Entity 2',
          type: 'glossaryNode',
          status: 'existing',
          urn: 'urn:li:glossaryNode:entity2',
          parentNames: [],
          parentUrns: [],
          level: 0,
          data: {} as any
        }
      ];

      const urnMap = createExistingEntityUrnMap(entities);
      
      expect(urnMap.size).toBe(4); // 2 by ID + 2 by name
      expect(urnMap.get('1')).toBe('urn:li:glossaryTerm:entity1');
      expect(urnMap.get('2')).toBe('urn:li:glossaryNode:entity2');
      expect(urnMap.get('entity 1')).toBe('urn:li:glossaryTerm:entity1');
      expect(urnMap.get('entity 2')).toBe('urn:li:glossaryNode:entity2');
    });
  });

  describe('AUTO_GENERATE_ALLOWED_ENTITY_TYPES', () => {
    it('should contain expected entity types', () => {
      expect(AUTO_GENERATE_ALLOWED_ENTITY_TYPES.has('glossaryTerm')).toBe(true);
      expect(AUTO_GENERATE_ALLOWED_ENTITY_TYPES.has('glossaryNode')).toBe(true);
      expect(AUTO_GENERATE_ALLOWED_ENTITY_TYPES.has('ownershipType')).toBe(true);
      expect(AUTO_GENERATE_ALLOWED_ENTITY_TYPES.has('container')).toBe(true);
      expect(AUTO_GENERATE_ALLOWED_ENTITY_TYPES.has('notebook')).toBe(true);
      expect(AUTO_GENERATE_ALLOWED_ENTITY_TYPES.has('domain')).toBe(true);
      expect(AUTO_GENERATE_ALLOWED_ENTITY_TYPES.has('dataProduct')).toBe(true);
    });
  });
});
