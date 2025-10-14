/**
 * Tests for glossary.utils.ts
 */

import { describe, it, expect } from 'vitest';
import { findDuplicateNames } from '../glossary.utils';
import { EntityData } from '../glossary.types';

describe('findDuplicateNames', () => {
  const createEntityData = (name: string, parentNodes?: string): EntityData => ({
    entity_type: 'glossaryTerm' as const,
    urn: '',
    name,
    description: '',
    term_source: 'INTERNAL',
    source_ref: '',
    source_url: '',
    ownership_users: '',
    ownership_groups: '',
    parent_nodes: parentNodes || '',
    related_contains: '',
    related_inherits: '',
    domain_urn: '',
    domain_name: '',
    custom_properties: '',
    status: 'new'
  });

  describe('Root level duplicates', () => {
    it('should detect duplicates at root level', () => {
      const entities: EntityData[] = [
        createEntityData('Customer'),
        createEntityData('Product'),
        createEntityData('Customer'), // Duplicate
      ];

      const result = findDuplicateNames(entities);

      expect(result.isValid).toBe(false);
      expect(result.errors).toHaveLength(2); // One error for each occurrence
      expect(result.errors[0].code).toBe('DUPLICATE_NAME');
      expect(result.errors[0].message).toContain('customer'); // lowercase (case-insensitive)
      expect(result.errors[0].message).toContain('at root level');
      expect(result.errors[0].message).toContain('rows 1, 3');
    });

    it('should not flag unique names at root level', () => {
      const entities: EntityData[] = [
        createEntityData('Customer'),
        createEntityData('Product'),
        createEntityData('Order'),
      ];

      const result = findDuplicateNames(entities);

      expect(result.isValid).toBe(true);
      expect(result.errors).toHaveLength(0);
    });
  });

  describe('Hierarchy-aware duplicate detection', () => {
    it('should allow same names under different parents', () => {
      const entities: EntityData[] = [
        createEntityData('Customer Name', 'Business Terms'),
        createEntityData('Customer Name', 'Technical Terms'), // Same name, different parent - OK
        createEntityData('Product Name', 'Business Terms'),
      ];

      const result = findDuplicateNames(entities);

      expect(result.isValid).toBe(true);
      expect(result.errors).toHaveLength(0);
    });

    it('should detect duplicates under same parent', () => {
      const entities: EntityData[] = [
        createEntityData('Customer Name', 'Business Terms'),
        createEntityData('Product Name', 'Business Terms'),
        createEntityData('Customer Name', 'Business Terms'), // Duplicate under same parent
      ];

      const result = findDuplicateNames(entities);

      expect(result.isValid).toBe(false);
      expect(result.errors).toHaveLength(2); // One error for each occurrence
      expect(result.errors[0].code).toBe('DUPLICATE_NAME');
      expect(result.errors[0].message).toContain('customer name'); // lowercase (case-insensitive)
      expect(result.errors[0].message).toContain('with parent(s)');
      expect(result.errors[0].message).toContain('business terms');
      expect(result.errors[0].message).toContain('rows 1, 3');
    });

    it('should handle multiple parents (comma-separated)', () => {
      const entities: EntityData[] = [
        createEntityData('Term A', 'Parent1,Parent2'),
        createEntityData('Term B', 'Parent1,Parent2'),
        createEntityData('Term A', 'Parent1,Parent2'), // Duplicate with same parents
        createEntityData('Term A', 'Parent1,Parent3'), // Different parent set - OK
      ];

      const result = findDuplicateNames(entities);

      expect(result.isValid).toBe(false);
      expect(result.errors).toHaveLength(2); // Only the first 3 entities (rows 1 & 3)
      expect(result.errors[0].message).toContain('rows 1, 3');
    });

    it('should handle hierarchical parent names', () => {
      const entities: EntityData[] = [
        createEntityData('Customer', 'Business Terms.Core Concepts'),
        createEntityData('Product', 'Business Terms.Core Concepts'),
        createEntityData('Customer', 'Business Terms.Core Concepts'), // Duplicate
        createEntityData('Customer', 'Technical Terms'), // Different parent - OK
      ];

      const result = findDuplicateNames(entities);

      expect(result.isValid).toBe(false);
      expect(result.errors).toHaveLength(2);
      expect(result.errors[0].message).toContain('rows 1, 3');
    });
  });

  describe('Mixed hierarchy levels', () => {
    it('should handle mix of root and child entities', () => {
      const entities: EntityData[] = [
        createEntityData('Customer'), // Root level
        createEntityData('Customer', 'Business Terms'), // Child - same name, different level - OK
        createEntityData('Product'), // Root level
        createEntityData('Customer'), // Root level duplicate
      ];

      const result = findDuplicateNames(entities);

      expect(result.isValid).toBe(false);
      expect(result.errors).toHaveLength(2); // Only root level duplicates (rows 1 & 4)
      expect(result.errors[0].message).toContain('at root level');
      expect(result.errors[0].message).toContain('rows 1, 4');
    });

    it('should handle complex nested hierarchies', () => {
      const entities: EntityData[] = [
        // Level 1
        createEntityData('Business Terms'),
        createEntityData('Technical Terms'),
        
        // Level 2 under Business Terms
        createEntityData('Customer', 'Business Terms'),
        createEntityData('Product', 'Business Terms'),
        
        // Level 2 under Technical Terms
        createEntityData('Customer', 'Technical Terms'), // Same name, different parent - OK
        createEntityData('Product', 'Technical Terms'), // Same name, different parent - OK
        
        // Duplicate under Business Terms
        createEntityData('Customer', 'Business Terms'), // Duplicate!
      ];

      const result = findDuplicateNames(entities);

      expect(result.isValid).toBe(false);
      expect(result.errors).toHaveLength(2); // Rows 3 & 7
      expect(result.errors[0].message).toContain('with parent(s) "business terms"');
    });
  });

  describe('Case insensitivity', () => {
    it('should treat names case-insensitively', () => {
      const entities: EntityData[] = [
        createEntityData('Customer Name', 'Business Terms'),
        createEntityData('CUSTOMER NAME', 'Business Terms'), // Same name, different case - duplicate
      ];

      const result = findDuplicateNames(entities);

      expect(result.isValid).toBe(false);
      expect(result.errors).toHaveLength(2);
      expect(result.errors[0].message).toContain('customer name');
    });

    it('should treat parent names case-insensitively', () => {
      const entities: EntityData[] = [
        createEntityData('Customer', 'business terms'),
        createEntityData('Product', 'BUSINESS TERMS'),
        createEntityData('Customer', 'Business Terms'), // Duplicate (case-insensitive parent match)
      ];

      const result = findDuplicateNames(entities);

      expect(result.isValid).toBe(false);
      expect(result.errors).toHaveLength(2);
    });
  });

  describe('Edge cases', () => {
    it('should handle empty entity list', () => {
      const result = findDuplicateNames([]);

      expect(result.isValid).toBe(true);
      expect(result.errors).toHaveLength(0);
    });

    it('should handle entities with empty names', () => {
      const entities: EntityData[] = [
        createEntityData(''),
        createEntityData('Customer'),
        createEntityData(''),
      ];

      const result = findDuplicateNames(entities);

      expect(result.isValid).toBe(true); // Empty names are ignored
      expect(result.errors).toHaveLength(0);
    });

    it('should handle entities with undefined names', () => {
      const entity: EntityData = createEntityData('Customer');
      entity.name = undefined as any;

      const entities: EntityData[] = [
        entity,
        createEntityData('Product'),
      ];

      const result = findDuplicateNames(entities);

      expect(result.isValid).toBe(true);
      expect(result.errors).toHaveLength(0);
    });

    it('should handle whitespace in parent names', () => {
      const entities: EntityData[] = [
        createEntityData('Customer', ' Business Terms '),
        createEntityData('Customer', 'Business Terms'), // Should be treated as same parent
      ];

      const result = findDuplicateNames(entities);

      // Note: Current implementation doesn't trim, so these would be different parents
      // If we want trimming, we'd need to update the function
      expect(result.isValid).toBe(false); // They should be duplicates
    });
  });

  describe('Multiple duplicates', () => {
    it('should detect multiple sets of duplicates', () => {
      const entities: EntityData[] = [
        createEntityData('Customer', 'Business Terms'),
        createEntityData('Product', 'Business Terms'),
        createEntityData('Customer', 'Business Terms'), // Duplicate 1
        createEntityData('Product', 'Business Terms'), // Duplicate 2
      ];

      const result = findDuplicateNames(entities);

      expect(result.isValid).toBe(false);
      expect(result.errors).toHaveLength(4); // 2 errors per duplicate pair
    });

    it('should detect triplicate names', () => {
      const entities: EntityData[] = [
        createEntityData('Customer', 'Business Terms'),
        createEntityData('Customer', 'Business Terms'),
        createEntityData('Customer', 'Business Terms'),
      ];

      const result = findDuplicateNames(entities);

      expect(result.isValid).toBe(false);
      expect(result.errors).toHaveLength(3); // One error for each occurrence
      expect(result.errors[0].message).toContain('rows 1, 2, 3');
    });
  });
});

