import { describe, it, expect } from 'vitest';
import {
  parseOwnershipFromColumns,
  parseOwnershipString,
  validateOwnershipColumns,
  createOwnershipPatchOperations,
  ParsedOwnership
} from '../ownershipParsingUtils';

describe('ownershipParsingUtils - New Column Format', () => {
  describe('parseOwnershipFromColumns', () => {
    it('should parse user ownership from users column', () => {
      const result = parseOwnershipFromColumns('admin:DEVELOPER', '');
      
      expect(result).toHaveLength(1);
      expect(result[0]).toEqual({
        ownershipTypeName: 'DEVELOPER',
        ownerName: 'admin',
        corpType: 'CORP_USER',
        ownerUrn: 'urn:li:corpuser:admin',
        ownerType: 'NONE'
      });
    });

    it('should parse group ownership from groups column', () => {
      const result = parseOwnershipFromColumns('', 'bfoo:Technical Owner');
      
      expect(result).toHaveLength(1);
      expect(result[0]).toEqual({
        ownershipTypeName: 'Technical Owner',
        ownerName: 'bfoo',
        corpType: 'CORP_GROUP',
        ownerUrn: 'urn:li:corpGroup:bfoo',
        ownerType: 'TECHNICAL_OWNER'
      });
    });

    it('should parse multiple users from users column', () => {
      const result = parseOwnershipFromColumns('admin:DEVELOPER|datahub:Technical Owner', '');
      
      expect(result).toHaveLength(2);
      expect(result[0].ownerName).toBe('admin');
      expect(result[0].ownershipTypeName).toBe('DEVELOPER');
      expect(result[0].corpType).toBe('CORP_USER');
      expect(result[1].ownerName).toBe('datahub');
      expect(result[1].ownershipTypeName).toBe('Technical Owner');
      expect(result[1].corpType).toBe('CORP_USER');
    });

    it('should parse multiple groups from groups column', () => {
      const result = parseOwnershipFromColumns('', 'bfoo:Technical Owner|datahub:Business Owner');
      
      expect(result).toHaveLength(2);
      expect(result[0].ownerName).toBe('bfoo');
      expect(result[0].ownershipTypeName).toBe('Technical Owner');
      expect(result[0].corpType).toBe('CORP_GROUP');
      expect(result[1].ownerName).toBe('datahub');
      expect(result[1].ownershipTypeName).toBe('Business Owner');
      expect(result[1].corpType).toBe('CORP_GROUP');
    });

    it('should parse mixed users and groups', () => {
      const result = parseOwnershipFromColumns('admin:DEVELOPER', 'bfoo:Technical Owner');
      
      expect(result).toHaveLength(2);
      expect(result[0]).toEqual({
        ownershipTypeName: 'DEVELOPER',
        ownerName: 'admin',
        corpType: 'CORP_USER',
        ownerUrn: 'urn:li:corpuser:admin',
        ownerType: 'NONE'
      });
      expect(result[1]).toEqual({
        ownershipTypeName: 'Technical Owner',
        ownerName: 'bfoo',
        corpType: 'CORP_GROUP',
        ownerUrn: 'urn:li:corpGroup:bfoo',
        ownerType: 'TECHNICAL_OWNER'
      });
    });

    it('should handle empty columns', () => {
      expect(parseOwnershipFromColumns('', '')).toEqual([]);
      expect(parseOwnershipFromColumns('   ', '   ')).toEqual([]);
    });

    it('should preserve existing URNs', () => {
      const result = parseOwnershipFromColumns('urn:li:corpuser:existing:DEVELOPER', '');
      
      expect(result).toHaveLength(1);
      expect(result[0]).toEqual({
        ownershipTypeName: 'DEVELOPER',
        ownerName: 'urn:li:corpuser:existing',
        corpType: 'CORP_USER',
        ownerUrn: 'urn:li:corpuser:existing',
        ownerType: 'NONE'
      });
    });

    it('should determine owner type based on ownership type name', () => {
      const technicalResult = parseOwnershipFromColumns('owner:technical', '');
      const businessResult = parseOwnershipFromColumns('owner:business', '');
      const dataResult = parseOwnershipFromColumns('owner:data steward', '');
      const otherResult = parseOwnershipFromColumns('owner:other', '');

      expect(technicalResult[0].ownerType).toBe('TECHNICAL_OWNER');
      expect(businessResult[0].ownerType).toBe('BUSINESS_OWNER');
      expect(dataResult[0].ownerType).toBe('DATA_STEWARD');
      expect(otherResult[0].ownerType).toBe('NONE');
    });
  });

  describe('validateOwnershipColumns', () => {
    it('should validate empty columns', () => {
      const result = validateOwnershipColumns('', '');
      expect(result.isValid).toBe(true);
      expect(result.error).toBeUndefined();
    });

    it('should validate valid user ownership', () => {
      const result = validateOwnershipColumns('admin:DEVELOPER', '');
      expect(result.isValid).toBe(true);
      expect(result.error).toBeUndefined();
    });

    it('should validate valid group ownership', () => {
      const result = validateOwnershipColumns('', 'bfoo:Technical Owner');
      expect(result.isValid).toBe(true);
      expect(result.error).toBeUndefined();
    });

    it('should validate mixed ownership', () => {
      const result = validateOwnershipColumns('admin:DEVELOPER', 'bfoo:Technical Owner');
      expect(result.isValid).toBe(true);
      expect(result.error).toBeUndefined();
    });

    it('should reject invalid user ownership format', () => {
      const result = validateOwnershipColumns('invalid', '');
      expect(result.isValid).toBe(false);
      expect(result.error).toContain('Invalid user ownership format');
    });

    it('should reject invalid group ownership format', () => {
      const result = validateOwnershipColumns('', 'invalid');
      expect(result.isValid).toBe(false);
      expect(result.error).toContain('Invalid group ownership format');
    });

    it('should reject empty ownership entries', () => {
      const result = validateOwnershipColumns('admin:DEVELOPER||', '');
      expect(result.isValid).toBe(false);
      expect(result.error).toContain('Empty ownership entries are not allowed');
    });
  });

  describe('createOwnershipPatchOperations', () => {
    const mockOwnershipTypeMap = new Map([
      ['developer', 'urn:li:ownershipType:developer-urn'],
      ['technical owner', 'urn:li:ownershipType:technical-urn'],
      ['business owner', 'urn:li:ownershipType:business-urn']
    ]);

    it('should create patch operations for user ownership', () => {
      const parsedOwnership: ParsedOwnership[] = [{
        ownershipTypeName: 'DEVELOPER',
        ownerName: 'admin',
        corpType: 'CORP_USER',
        ownerUrn: 'urn:li:corpuser:admin',
        ownerType: 'NONE'
      }];

      const result = createOwnershipPatchOperations(parsedOwnership, mockOwnershipTypeMap);

      expect(result).toHaveLength(1);
      expect(result[0]).toEqual({
        op: 'ADD',
        path: '/owners/urn:li:corpuser:admin/urn:li:ownershipType:developer-urn',
        value: JSON.stringify({
          owner: 'urn:li:corpuser:admin',
          typeUrn: 'urn:li:ownershipType:developer-urn',
          type: 'NONE',
          source: { type: 'MANUAL' }
        })
      });
    });

    it('should create patch operations for group ownership', () => {
      const parsedOwnership: ParsedOwnership[] = [{
        ownershipTypeName: 'Technical Owner',
        ownerName: 'bfoo',
        corpType: 'CORP_GROUP',
        ownerUrn: 'urn:li:corpGroup:bfoo',
        ownerType: 'NONE'
      }];

      const result = createOwnershipPatchOperations(parsedOwnership, mockOwnershipTypeMap);

      expect(result).toHaveLength(1);
      expect(result[0]).toEqual({
        op: 'ADD',
        path: '/owners/urn:li:corpGroup:bfoo/urn:li:ownershipType:technical-urn',
        value: JSON.stringify({
          owner: 'urn:li:corpGroup:bfoo',
          typeUrn: 'urn:li:ownershipType:technical-urn',
          type: 'NONE',
          source: { type: 'MANUAL' }
        })
      });
    });

    it('should create patch operations for mixed ownership', () => {
      const parsedOwnership: ParsedOwnership[] = [
        {
          ownershipTypeName: 'DEVELOPER',
          ownerName: 'admin',
          corpType: 'CORP_USER',
          ownerUrn: 'urn:li:corpuser:admin',
          ownerType: 'NONE'
        },
        {
          ownershipTypeName: 'Technical Owner',
          ownerName: 'bfoo',
          corpType: 'CORP_GROUP',
          ownerUrn: 'urn:li:corpGroup:bfoo',
          ownerType: 'NONE'
        }
      ];

      const result = createOwnershipPatchOperations(parsedOwnership, mockOwnershipTypeMap);

      expect(result).toHaveLength(2);
      expect(result[0].path).toBe('/owners/urn:li:corpuser:admin/urn:li:ownershipType:developer-urn');
      expect(result[1].path).toBe('/owners/urn:li:corpGroup:bfoo/urn:li:ownershipType:technical-urn');
    });
  });

  describe('integration tests', () => {
    it('should handle the complete workflow from CSV columns to patch operations', () => {
      const usersColumn = 'admin:DEVELOPER|datahub:Technical Owner';
      const groupsColumn = 'bfoo:Technical Owner';
      const ownershipTypeMap = new Map([
        ['developer', 'urn:li:ownershipType:developer-urn'],
        ['technical owner', 'urn:li:ownershipType:technical-urn']
      ]);

      // Parse the ownership columns
      const parsedOwnership = parseOwnershipFromColumns(usersColumn, groupsColumn);
      expect(parsedOwnership).toHaveLength(3);

      // Validate the parsed ownership
      const validation = validateOwnershipColumns(usersColumn, groupsColumn);
      expect(validation.isValid).toBe(true);

      // Create patch operations
      const patches = createOwnershipPatchOperations(parsedOwnership, ownershipTypeMap);
      expect(patches).toHaveLength(3);

      // Verify the patch operations
      expect(patches[0].path).toBe('/owners/urn:li:corpuser:admin/urn:li:ownershipType:developer-urn');
      expect(patches[1].path).toBe('/owners/urn:li:corpuser:datahub/urn:li:ownershipType:technical-urn');
      expect(patches[2].path).toBe('/owners/urn:li:corpGroup:bfoo/urn:li:ownershipType:technical-urn');
    });
  });
});
