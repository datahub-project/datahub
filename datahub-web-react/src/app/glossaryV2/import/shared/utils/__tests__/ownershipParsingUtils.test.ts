import { describe, it, expect } from 'vitest';
import {
  parseSingleOwnership,
  parseOwnershipString,
  validateOwnershipString,
  createOwnershipPatchOperations,
  ParsedOwnership
} from '../ownershipParsingUtils';

describe('ownershipParsingUtils', () => {
  describe('parseSingleOwnership', () => {
    it('should parse a valid ownership string with CORP_USER', () => {
      const result = parseSingleOwnership('admin:DEVELOPER', 'CORP_USER');
      
      expect(result).toEqual({
        ownershipTypeName: 'DEVELOPER',
        ownerName: 'admin',
        corpType: 'CORP_USER',
        ownerUrn: 'urn:li:corpuser:admin',
        ownerType: 'NONE'
      });
    });

    it('should parse a valid ownership string with CORP_GROUP', () => {
      const result = parseSingleOwnership('bfoo:Technical Owner', 'CORP_GROUP');
      
      expect(result).toEqual({
        ownershipTypeName: 'Technical Owner',
        ownerName: 'bfoo',
        corpType: 'CORP_GROUP',
        ownerUrn: 'urn:li:corpGroup:bfoo',
        ownerType: 'TECHNICAL_OWNER'
      });
    });

    it('should default to CORP_USER when corpType is not provided', () => {
      const result = parseSingleOwnership('admin:DEVELOPER');
      
      expect(result).toEqual({
        ownershipTypeName: 'DEVELOPER',
        ownerName: 'admin',
        corpType: 'CORP_USER',
        ownerUrn: 'urn:li:corpuser:admin',
        ownerType: 'NONE'
      });
    });

    it('should preserve existing URNs', () => {
      const result = parseSingleOwnership('urn:li:corpuser:existing:DEVELOPER');
      
      expect(result).toEqual({
        ownershipTypeName: 'DEVELOPER',
        ownerName: 'urn:li:corpuser:existing',
        corpType: 'CORP_USER',
        ownerUrn: 'urn:li:corpuser:existing',
        ownerType: 'NONE'
      });
    });

    it('should determine owner type based on ownership type name', () => {
      const technicalResult = parseSingleOwnership('owner:technical');
      const businessResult = parseSingleOwnership('owner:business');
      const dataResult = parseSingleOwnership('owner:data');
      const otherResult = parseSingleOwnership('owner:other');

      expect(technicalResult?.ownerType).toBe('TECHNICAL_OWNER');
      expect(businessResult?.ownerType).toBe('BUSINESS_OWNER');
      expect(dataResult?.ownerType).toBe('DATA_STEWARD');
      expect(otherResult?.ownerType).toBe('NONE');
    });

    it('should return null for invalid ownership strings', () => {
      expect(parseSingleOwnership('')).toBeNull();
      expect(parseSingleOwnership('   ')).toBeNull();
      expect(parseSingleOwnership('invalid')).toBeNull();
      expect(parseSingleOwnership(':owner')).toBeNull();
      expect(parseSingleOwnership('type:')).toBeNull();
    });

    it('should handle case-insensitive owner type detection', () => {
      const technicalResult = parseSingleOwnership('owner:TECHNICAL');
      const businessResult = parseSingleOwnership('owner:Business');
      const dataResult = parseSingleOwnership('owner:DATA_STEWARD');

      expect(technicalResult?.ownerType).toBe('TECHNICAL_OWNER');
      expect(businessResult?.ownerType).toBe('BUSINESS_OWNER');
      expect(dataResult?.ownerType).toBe('DATA_STEWARD');
    });
  });

  describe('parseOwnershipString', () => {
    it('should parse single ownership string', () => {
      const result = parseOwnershipString('admin:DEVELOPER');
      
      expect(result).toHaveLength(1);
      expect(result[0]).toEqual({
        ownershipTypeName: 'DEVELOPER',
        ownerName: 'admin',
        corpType: 'CORP_USER',
        ownerUrn: 'urn:li:corpuser:admin',
        ownerType: 'NONE'
      });
    });

    it('should parse multiple ownership strings separated by comma', () => {
      const result = parseOwnershipString('bfoo:Technical Owner,datahub:Technical Owner');
      
      expect(result).toHaveLength(2);
      expect(result[0]).toEqual({
        ownershipTypeName: 'Technical Owner',
        ownerName: 'bfoo',
        corpType: 'CORP_USER',
        ownerUrn: 'urn:li:corpuser:bfoo',
        ownerType: 'TECHNICAL_OWNER'
      });
      expect(result[1]).toEqual({
        ownershipTypeName: 'Technical Owner',
        ownerName: 'datahub',
        corpType: 'CORP_USER',
        ownerUrn: 'urn:li:corpuser:datahub',
        ownerType: 'TECHNICAL_OWNER'
      });
    });

    it('should parse multiple ownership strings separated by pipe', () => {
      const result = parseOwnershipString('bfoo:Technical Owner|datahub:Technical Owner');
      
      expect(result).toHaveLength(2);
      expect(result[0]).toEqual({
        ownershipTypeName: 'Technical Owner',
        ownerName: 'bfoo',
        corpType: 'CORP_USER',
        ownerUrn: 'urn:li:corpuser:bfoo',
        ownerType: 'TECHNICAL_OWNER'
      });
      expect(result[1]).toEqual({
        ownershipTypeName: 'Technical Owner',
        ownerName: 'datahub',
        corpType: 'CORP_USER',
        ownerUrn: 'urn:li:corpuser:datahub',
        ownerType: 'TECHNICAL_OWNER'
      });
    });

    it('should handle mixed separators', () => {
      const result = parseOwnershipString('admin:DEVELOPER|bfoo:Technical Owner,datahub:Technical Owner');
      
      expect(result).toHaveLength(3);
      expect(result[0].ownershipTypeName).toBe('DEVELOPER');
      expect(result[1].ownershipTypeName).toBe('Technical Owner');
      expect(result[2].ownershipTypeName).toBe('Technical Owner');
    });

    it('should filter out invalid entries', () => {
      const result = parseOwnershipString('admin:DEVELOPER,invalid,datahub:Technical Owner');
      
      expect(result).toHaveLength(2);
      expect(result[0].ownershipTypeName).toBe('DEVELOPER');
      expect(result[1].ownershipTypeName).toBe('Technical Owner');
    });

    it('should handle empty and whitespace-only strings', () => {
      expect(parseOwnershipString('')).toEqual([]);
      expect(parseOwnershipString('   ')).toEqual([]);
      expect(parseOwnershipString('admin:DEVELOPER,   ,datahub:Technical Owner')).toHaveLength(2);
    });

    it('should trim whitespace from entries', () => {
      const result = parseOwnershipString(' admin : DEVELOPER | datahub : Technical Owner ');
      
      expect(result).toHaveLength(2);
      expect(result[0].ownershipTypeName).toBe('DEVELOPER');
      expect(result[0].ownerName).toBe('admin');
      expect(result[1].ownershipTypeName).toBe('Technical Owner');
      expect(result[1].ownerName).toBe('datahub');
    });
  });

  describe('validateOwnershipString', () => {
    it('should validate empty ownership string', () => {
      const result = validateOwnershipString('');
      expect(result.isValid).toBe(true);
      expect(result.error).toBeUndefined();
    });

    it('should validate valid ownership string', () => {
      const result = validateOwnershipString('admin:DEVELOPER:CORP_USER');
      expect(result.isValid).toBe(true);
      expect(result.error).toBeUndefined();
    });

    it('should validate multiple ownership strings', () => {
      const result = validateOwnershipString('admin:DEVELOPER:CORP_USER|datahub:Technical Owner:CORP_USER');
      expect(result.isValid).toBe(true);
      expect(result.error).toBeUndefined();
    });

    it('should reject invalid ownership string', () => {
      const result = validateOwnershipString('invalid');
      expect(result.isValid).toBe(false);
      expect(result.error).toContain('Invalid ownership format');
    });

    it('should reject empty ownership entries', () => {
      const result = validateOwnershipString('admin:DEVELOPER:CORP_USER,,datahub:Technical Owner:CORP_USER');
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

    it('should create patch operations for single ownership', () => {
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

    it('should create patch operations for multiple ownerships', () => {
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
          ownerType: 'TECHNICAL_OWNER'
        }
      ];

      const result = createOwnershipPatchOperations(parsedOwnership, mockOwnershipTypeMap);

      expect(result).toHaveLength(2);
      expect(result[0].path).toBe('/owners/urn:li:corpuser:admin/urn:li:ownershipType:developer-urn');
      expect(result[1].path).toBe('/owners/urn:li:corpGroup:bfoo/urn:li:ownershipType:technical-urn');
    });

    it('should throw error for missing ownership type', () => {
      const parsedOwnership: ParsedOwnership[] = [{
        ownershipTypeName: 'unknown',
        ownerName: 'DEVELOPER',
        corpType: 'CORP_USER',
        ownerUrn: 'urn:li:corpuser:DEVELOPER',
        ownerType: 'NONE'
      }];

      expect(() => {
        createOwnershipPatchOperations(parsedOwnership, mockOwnershipTypeMap);
      }).toThrow('Ownership type "unknown" not found in ownership type map');
    });

    it('should handle case-insensitive ownership type lookup', () => {
      const parsedOwnership: ParsedOwnership[] = [{
        ownershipTypeName: 'UNKNOWN', // not in map
        ownerName: 'DEVELOPER',
        corpType: 'CORP_USER',
        ownerUrn: 'urn:li:corpuser:DEVELOPER',
        ownerType: 'NONE'
      }];

      // This should throw because the ownership type is not in the map
      expect(() => {
        createOwnershipPatchOperations(parsedOwnership, mockOwnershipTypeMap);
      }).toThrow('Ownership type "UNKNOWN" not found in ownership type map');
    });
  });

  describe('integration tests', () => {
    it('should handle the complete workflow from CSV string to patch operations', () => {
      const csvOwnershipString = 'bfoo:Technical Owner|datahub:Technical Owner';
      const ownershipTypeMap = new Map([
        ['technical owner', 'urn:li:ownershipType:technical-urn']
      ]);

      // Parse the ownership string
      const parsedOwnership = parseOwnershipString(csvOwnershipString);
      expect(parsedOwnership).toHaveLength(2);

      // Validate the parsed ownership
      const validation = validateOwnershipString(csvOwnershipString);
      expect(validation.isValid).toBe(true);

      // Create patch operations
      const patches = createOwnershipPatchOperations(parsedOwnership, ownershipTypeMap);
      expect(patches).toHaveLength(2);

      // Verify the patch operations
      expect(patches[0].path).toBe('/owners/urn:li:corpuser:bfoo/urn:li:ownershipType:technical-urn');
      expect(patches[1].path).toBe('/owners/urn:li:corpuser:datahub/urn:li:ownershipType:technical-urn');
    });

    it('should handle real-world CSV data from the updated file', () => {
      const csvOwnershipString = 'bfoo:Technical Owner|datahub:Technical Owner';
      const ownershipTypeMap = new Map([
        ['technical owner', 'urn:li:ownershipType:09d4bbe7-d39f-4fd7-9f64-95da1bc86ecc']
      ]);

      const parsedOwnership = parseOwnershipString(csvOwnershipString);
      const patches = createOwnershipPatchOperations(parsedOwnership, ownershipTypeMap);

      expect(parsedOwnership).toHaveLength(2);
      expect(patches).toHaveLength(2);

      // Verify the first ownership
      expect(parsedOwnership[0].ownerUrn).toBe('urn:li:corpuser:bfoo');
      expect(parsedOwnership[0].ownerType).toBe('TECHNICAL_OWNER');

      // Verify the second ownership
      expect(parsedOwnership[1].ownerUrn).toBe('urn:li:corpuser:datahub');
      expect(parsedOwnership[1].ownerType).toBe('TECHNICAL_OWNER');
    });
  });
});
