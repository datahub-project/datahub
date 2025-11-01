/**
 * Tests for HierarchyNameResolver
 */

import { describe, it, expect } from 'vitest';
import { HierarchyNameResolver } from '../hierarchyUtils';
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

const mockEntities: Entity[] = [
  {
    id: 'parent-1',
    name: 'Business Terms',
    type: 'glossaryNode',
    status: 'existing',
    parentNames: [],
    parentUrns: [],
    level: 0,
    data: { ...mockEntityData, name: 'Business Terms' },
    urn: 'urn:li:glossaryNode:business-terms'
  },
  {
    id: 'parent-2',
    name: 'Business Terms Nested',
    type: 'glossaryNode',
    status: 'existing',
    parentNames: ['Business Terms'],
    parentUrns: ['urn:li:glossaryNode:business-terms'],
    level: 1,
    data: { ...mockEntityData, name: 'Business Terms Nested' },
    urn: 'urn:li:glossaryNode:business-terms-nested'
  },
  {
    id: 'child-1',
    name: 'Customer Name',
    type: 'glossaryTerm',
    status: 'new',
    parentNames: ['Business Terms.Business Terms Nested'],
    parentUrns: [],
    level: 2,
    data: { ...mockEntityData, name: 'Customer Name' }
  }
];

describe('HierarchyNameResolver', () => {
  describe('parseHierarchicalName', () => {
    it('should return the last part of a hierarchical name', () => {
      expect(HierarchyNameResolver.parseHierarchicalName('Business Terms.Business Terms Nested'))
        .toBe('Business Terms Nested');
    });

    it('should return the original name if no dots', () => {
      expect(HierarchyNameResolver.parseHierarchicalName('Simple Name'))
        .toBe('Simple Name');
    });

    it('should handle multiple levels', () => {
      expect(HierarchyNameResolver.parseHierarchicalName('Level1.Level2.Level3'))
        .toBe('Level3');
    });

    it('should handle empty or whitespace names', () => {
      expect(HierarchyNameResolver.parseHierarchicalName(''))
        .toBe('');
      expect(HierarchyNameResolver.parseHierarchicalName('   '))
        .toBe('');
    });

    it('should trim whitespace from parts', () => {
      expect(HierarchyNameResolver.parseHierarchicalName(' Parent . Child '))
        .toBe('Child');
    });

    it('should filter out empty parts', () => {
      expect(HierarchyNameResolver.parseHierarchicalName('Parent..Child'))
        .toBe('Child');
    });
  });

  describe('findParentEntity', () => {
    it('should find parent by hierarchical name', () => {
      const result = HierarchyNameResolver.findParentEntity(
        'Business Terms.Business Terms Nested',
        mockEntities
      );
      expect(result).toBeDefined();
      expect(result?.name).toBe('Business Terms Nested');
    });

    it('should find parent by simple name', () => {
      const result = HierarchyNameResolver.findParentEntity(
        'Business Terms',
        mockEntities
      );
      expect(result).toBeDefined();
      expect(result?.name).toBe('Business Terms');
    });

    it('should return undefined if parent not found', () => {
      const result = HierarchyNameResolver.findParentEntity(
        'Non Existent Parent',
        mockEntities
      );
      expect(result).toBeUndefined();
    });

    it('should return undefined for empty parent name', () => {
      const result = HierarchyNameResolver.findParentEntity('', mockEntities);
      expect(result).toBeUndefined();
    });

    it('should return undefined for empty entities array', () => {
      const result = HierarchyNameResolver.findParentEntity('Business Terms', []);
      expect(result).toBeUndefined();
    });
  });

  describe('findParentEntityCaseInsensitive', () => {
    it('should find parent with case-insensitive search', () => {
      const result = HierarchyNameResolver.findParentEntityCaseInsensitive(
        'business terms',
        mockEntities
      );
      expect(result).toBeDefined();
      expect(result?.name).toBe('Business Terms');
    });

    it('should find parent with hierarchical name case-insensitive', () => {
      const result = HierarchyNameResolver.findParentEntityCaseInsensitive(
        'business terms.business terms nested',
        mockEntities
      );
      expect(result).toBeDefined();
      expect(result?.name).toBe('Business Terms Nested');
    });
  });

  describe('resolveParentUrns', () => {
    it('should resolve parent URNs for existing entities', () => {
      const generatedUrnMap = new Map<string, string>();
      const result = HierarchyNameResolver.resolveParentUrns(
        mockEntities[2], // Customer Name with parent "Business Terms.Business Terms Nested"
        mockEntities,
        generatedUrnMap
      );
      
      expect(result).toHaveLength(1);
      expect(result[0]).toBe('urn:li:glossaryNode:business-terms-nested');
    });

    it('should resolve parent URNs for new entities', () => {
      const newEntity: Entity = {
        id: 'new-child',
        name: 'New Child',
        type: 'glossaryTerm',
        status: 'new',
        parentNames: ['Business Terms'],
        parentUrns: [],
        level: 1,
        data: { ...mockEntityData, name: 'New Child' }
      };

      const generatedUrnMap = new Map<string, string>();
      generatedUrnMap.set('parent-1', 'urn:li:glossaryNode:generated-business-terms');

      const result = HierarchyNameResolver.resolveParentUrns(
        newEntity,
        [newEntity, ...mockEntities],
        generatedUrnMap
      );
      
      expect(result).toHaveLength(1);
      expect(result[0]).toBe('urn:li:glossaryNode:business-terms');
    });

    it('should handle missing parent entities', () => {
      const entityWithMissingParent: Entity = {
        id: 'orphan',
        name: 'Orphan Entity',
        type: 'glossaryTerm',
        status: 'new',
        parentNames: ['Missing Parent'],
        parentUrns: [],
        level: 0,
        data: { ...mockEntityData, name: 'Orphan Entity' }
      };

      const consoleSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
      
      const result = HierarchyNameResolver.resolveParentUrns(
        entityWithMissingParent,
        mockEntities,
        new Map()
      );
      
      expect(result).toHaveLength(0);
      expect(consoleSpy).toHaveBeenCalledWith(
        'Parent entity "Missing Parent" (resolved to "Missing Parent") not found for "Orphan Entity"'
      );
      
      consoleSpy.mockRestore();
    });
  });

  describe('isHierarchicalName', () => {
    it('should return true for hierarchical names', () => {
      expect(HierarchyNameResolver.isHierarchicalName('Parent.Child')).toBe(true);
      expect(HierarchyNameResolver.isHierarchicalName('A.B.C')).toBe(true);
    });

    it('should return false for simple names', () => {
      expect(HierarchyNameResolver.isHierarchicalName('Simple')).toBe(false);
      expect(HierarchyNameResolver.isHierarchicalName('')).toBe(false);
    });
  });

  describe('getPathParts', () => {
    it('should split hierarchical name into parts', () => {
      expect(HierarchyNameResolver.getPathParts('Parent.Child'))
        .toEqual(['Parent', 'Child']);
    });

    it('should handle multiple levels', () => {
      expect(HierarchyNameResolver.getPathParts('A.B.C.D'))
        .toEqual(['A', 'B', 'C', 'D']);
    });

    it('should trim whitespace and filter empty parts', () => {
      expect(HierarchyNameResolver.getPathParts(' Parent . . Child '))
        .toEqual(['Parent', 'Child']);
    });

    it('should return empty array for empty string', () => {
      expect(HierarchyNameResolver.getPathParts('')).toEqual([]);
    });
  });

  describe('buildHierarchicalName', () => {
    it('should build hierarchical name from parts', () => {
      expect(HierarchyNameResolver.buildHierarchicalName(['Parent', 'Child']))
        .toBe('Parent.Child');
    });

    it('should filter out empty parts', () => {
      expect(HierarchyNameResolver.buildHierarchicalName(['Parent', '', 'Child']))
        .toBe('Parent.Child');
    });

    it('should return empty string for empty array', () => {
      expect(HierarchyNameResolver.buildHierarchicalName([])).toBe('');
    });
  });
});
