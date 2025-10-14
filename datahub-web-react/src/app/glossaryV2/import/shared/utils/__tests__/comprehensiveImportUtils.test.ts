/**
 * Tests for Comprehensive Import Utilities
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  createComprehensiveImportPlan,
  convertPlanToPatchInputs,
  ComprehensivePatchInput,
  OwnershipTypeInput
} from '../comprehensiveImportUtils';
import { Entity, EntityData } from '../../../glossary.types';

// Mock data
const mockEntityData: EntityData = {
  entity_type: 'glossaryTerm',
  urn: '',
  name: 'Test Term',
  description: 'Test Description',
  term_source: 'INTERNAL',
  source_ref: '',
  source_url: '',
  ownership_users: 'admin:DEVELOPER',
  ownership_groups: 'bfoo:Technical Owner',
  parent_nodes: '',
  related_contains: '',
  related_inherits: '',
  domain_urn: '',
  domain_name: '',
  custom_properties: '{"key1":"value1","key2":"value2"}'
};

const mockEntity: Entity = {
  id: 'test-1',
  name: 'Test Term',
  type: 'glossaryTerm',
  status: 'new',
  parentNames: [],
  parentUrns: [],
  level: 0,
  data: mockEntityData
};

const mockParentEntity: Entity = {
  id: 'parent-1',
  name: 'Parent Term',
  type: 'glossaryNode',
  status: 'new',
  parentNames: [],
  parentUrns: [],
  level: 0,
  data: {
    ...mockEntityData,
    entity_type: 'glossaryNode',
    name: 'Parent Term'
  }
};

const mockExistingEntity: Entity = {
  id: 'existing-1',
  name: 'Existing Term',
  type: 'glossaryTerm',
  status: 'existing',
  urn: 'urn:li:glossaryTerm:existing',
  parentNames: [],
  parentUrns: [],
  level: 0,
  data: {
    ...mockEntityData,
    name: 'Existing Term'
  }
};

describe('Comprehensive Import Utils', () => {
  let existingOwnershipTypes: Map<string, string>;

  beforeEach(() => {
    existingOwnershipTypes = new Map([
      ['developer', 'urn:li:ownershipType:developer'],
      ['technical owner', 'urn:li:ownershipType:technical-owner']
    ]);
  });

  describe('createComprehensiveImportPlan', () => {
    it('should create comprehensive import plan', () => {
      const entities = [mockEntity];
      const existingEntities = [mockExistingEntity];

      const plan = createComprehensiveImportPlan(entities, existingEntities, existingOwnershipTypes);

      expect(plan).toBeDefined();
      expect(plan.entities).toHaveLength(1);
      expect(plan.ownershipTypes).toHaveLength(0); // No new ownership types needed
      expect(plan.ownershipPatches).toHaveLength(1);
      expect(plan.parentRelationshipPatches).toHaveLength(0);
      expect(plan.relatedTermPatches).toHaveLength(0);
      expect(plan.domainAssignmentPatches).toHaveLength(0);
    });

    it('should extract new ownership types', () => {
      const entities = [mockEntity];
      const existingEntities = [mockExistingEntity];
      const emptyOwnershipTypes = new Map<string, string>();

      const plan = createComprehensiveImportPlan(entities, existingEntities, emptyOwnershipTypes);

      expect(plan.ownershipTypes).toHaveLength(2); // DEVELOPER and Technical Owner
      expect(plan.ownershipTypes.some(ot => ot.name === 'DEVELOPER')).toBe(true);
      expect(plan.ownershipTypes.some(ot => ot.name === 'Technical Owner')).toBe(true);
    });

    it('should handle hierarchical entities', () => {
      const childEntity: Entity = {
        ...mockEntity,
        id: 'child-1',
        name: 'Child Term',
        parentNames: ['Parent Term'],
        data: {
          ...mockEntityData,
          name: 'Child Term',
          parent_nodes: 'Parent Term'
        }
      };

      const entities = [mockParentEntity, childEntity];
      const existingEntities = [mockExistingEntity];

      const plan = createComprehensiveImportPlan(entities, existingEntities, existingOwnershipTypes);

      expect(plan.entities).toHaveLength(2);
      expect(plan.parentRelationshipPatches).toHaveLength(0); // Parent relationships are now handled in entity patches
    });

    it('should handle related terms', () => {
      const entityWithRelations: Entity = {
        ...mockEntity,
        data: {
          ...mockEntityData,
          related_contains: 'Related Term 1,Related Term 2',
          related_inherits: 'Inherited Term'
        }
      };

      const entities = [entityWithRelations];
      const existingEntities = [mockExistingEntity];

      const plan = createComprehensiveImportPlan(entities, existingEntities, existingOwnershipTypes);

      expect(plan.relatedTermPatches).toHaveLength(0); // No related terms in import batch
    });

    it('should handle domain assignments', () => {
      const entityWithDomain: Entity = {
        ...mockEntity,
        data: {
          ...mockEntityData,
          domain_urn: 'urn:li:domain:test-domain',
          domain_name: 'Test Domain'
        }
      };

      const entities = [entityWithDomain];
      const existingEntities = [mockExistingEntity];

      const plan = createComprehensiveImportPlan(entities, existingEntities, existingOwnershipTypes);

      expect(plan.domainAssignmentPatches).toHaveLength(1);
    });
  });

  describe('convertPlanToPatchInputs', () => {
    it('should convert plan to patch inputs in correct order', () => {
      const plan = {
        ownershipTypes: [
          { name: 'New Type', description: 'New Description', urn: 'urn:li:ownershipType:new-type' }
        ],
        entities: [mockEntity],
        ownershipPatches: [],
        parentRelationshipPatches: [],
        relatedTermPatches: [],
        domainAssignmentPatches: [],
        urnMap: new Map([['test-1', 'urn:li:glossaryTerm:test-1']])
      };

      const patchInputs = convertPlanToPatchInputs(plan, [mockEntity], []);

      expect(patchInputs).toBeDefined();
      expect(patchInputs.length).toBeGreaterThan(0);
      
      // First should be ownership type
      expect(patchInputs[0].entityType).toBe('ownershipType');
      expect(patchInputs[0].aspectName).toBe('ownershipTypeInfo');
    });

    it('should create entity patches with correct structure', () => {
      const plan = {
        ownershipTypes: [],
        entities: [mockEntity],
        ownershipPatches: [],
        parentRelationshipPatches: [],
        relatedTermPatches: [],
        domainAssignmentPatches: [],
        urnMap: new Map([['test-1', 'urn:li:glossaryTerm:test-1']])
      };

      const patchInputs = convertPlanToPatchInputs(plan, [mockEntity], []);

      const entityPatch = patchInputs.find(p => p.entityType === 'glossaryTerm');
      expect(entityPatch).toBeDefined();
      expect(entityPatch?.aspectName).toBe('glossaryTermInfo');
      expect(entityPatch?.patch).toContainEqual({
        op: 'ADD',
        path: '/name',
        value: 'Test Term'
      });
      expect(entityPatch?.patch).toContainEqual({
        op: 'ADD',
        path: '/definition',
        value: 'Test Description'
      });
    });

    it('should handle custom properties', () => {
      const plan = {
        ownershipTypes: [],
        entities: [mockEntity],
        ownershipPatches: [],
        parentRelationshipPatches: [],
        relatedTermPatches: [],
        domainAssignmentPatches: [],
        urnMap: new Map([['test-1', 'urn:li:glossaryTerm:test-1']])
      };

      const patchInputs = convertPlanToPatchInputs(plan, [mockEntity], []);

      const entityPatch = patchInputs.find(p => p.entityType === 'glossaryTerm');
      expect(entityPatch?.patch).toContainEqual({
        op: 'ADD',
        path: '/customProperties/key1',
        value: '"value1"'
      });
      expect(entityPatch?.patch).toContainEqual({
        op: 'ADD',
        path: '/customProperties/key2',
        value: '"value2"'
      });
    });
  });

  describe('OwnershipTypeInput', () => {
    it('should have correct structure', () => {
      const ownershipType: OwnershipTypeInput = {
        name: 'Test Type',
        description: 'Test Description',
        urn: 'urn:li:ownershipType:test-type'
      };

      expect(ownershipType.name).toBe('Test Type');
      expect(ownershipType.description).toBe('Test Description');
      expect(ownershipType.urn).toBe('urn:li:ownershipType:test-type');
    });
  });

  describe('ComprehensivePatchInput', () => {
    it('should have correct structure', () => {
      const patchInput: ComprehensivePatchInput = {
        urn: 'urn:li:glossaryTerm:test',
        entityType: 'glossaryTerm',
        aspectName: 'glossaryTermInfo',
        patch: [
          { op: 'ADD', path: '/name', value: 'Test' }
        ],
        arrayPrimaryKeys: [
          { arrayField: 'owners', keys: ['owner1', 'owner2'] }
        ],
        forceGenericPatch: false
      };

      expect(patchInput.urn).toBe('urn:li:glossaryTerm:test');
      expect(patchInput.entityType).toBe('glossaryTerm');
      expect(patchInput.aspectName).toBe('glossaryTermInfo');
      expect(patchInput.patch).toHaveLength(1);
      expect(patchInput.arrayPrimaryKeys).toHaveLength(1);
      expect(patchInput.forceGenericPatch).toBe(false);
    });
  });
});
