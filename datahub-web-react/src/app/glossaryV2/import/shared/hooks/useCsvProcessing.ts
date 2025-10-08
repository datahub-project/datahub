/**
 * Hook for CSV processing and validation
 */

import { useCallback } from 'react';
import Papa from 'papaparse';
import { 
  EntityData, 
  CsvParseResult, 
  CsvError, 
  CsvWarning, 
  ValidationResult,
  UseCsvProcessingReturn 
} from '../../glossary.types';
import {
  validateEntityName,
  validateEntityType,
  validateParentNodes,
  validateDomainUrn,
  validateCustomProperties,
  validateUrl,
  findDuplicateNames
} from '../../glossary.utils';

export function useCsvProcessing(): UseCsvProcessingReturn {
  /**
   * Parse CSV text into structured data
   */
  const parseCsvText = useCallback((csvText: string): CsvParseResult => {
    const errors: CsvError[] = [];
    const warnings: CsvWarning[] = [];
    let data: EntityData[] = [];

    try {
      const result = Papa.parse(csvText, {
        header: true,
        skipEmptyLines: true,
        transformHeader: (header) => header.trim(),
        transform: (value, field) => {
          if (typeof value === 'string') {
            return value.trim();
          }
          return value;
        }
      });

      if (result.errors.length > 0) {
        result.errors.forEach(error => {
          errors.push({
            row: (error.row || 0) + 1, // Papa Parse uses 0-based indexing
            field: (error as any).field || 'general',
            message: error.message,
            type: 'format'
          });
        });
      }

      // Convert parsed data to EntityData format
      data = result.data.map((row: any, index: number) => {
        const entityData: EntityData = {
          entity_type: row.entity_type || '',
          urn: row.urn || '',
          name: row.name || '',
          description: row.description || '',
          term_source: row.term_source || '',
          source_ref: row.source_ref || '',
          source_url: row.source_url || '',
          ownership_users: row.ownership_users || '',
          ownership_groups: row.ownership_groups || '',
          parent_nodes: row.parent_nodes || '',
          related_contains: row.related_contains || '',
          related_inherits: row.related_inherits || '',
          domain_urn: row.domain_urn || '',
          domain_name: row.domain_name || '',
          custom_properties: row.custom_properties || '',
          status: row.status || ''
        };

        // Validate each field
        const nameValidation = validateEntityName(entityData.name);
        if (!nameValidation.isValid) {
          nameValidation.errors.forEach(error => {
            errors.push({
              row: index + 1,
              field: error.field,
              message: error.message,
              type: 'validation'
            });
          });
        }
        nameValidation.warnings.forEach(warning => {
          warnings.push({
            row: index + 1,
            field: warning.field,
            message: warning.message,
            type: 'suggestion'
          });
        });

        const typeValidation = validateEntityType(entityData.entity_type);
        if (!typeValidation.isValid) {
          typeValidation.errors.forEach(error => {
            errors.push({
              row: index + 1,
              field: error.field,
              message: error.message,
              type: 'validation'
            });
          });
        }

        // Validate other fields
        const parentValidation = validateParentNodes(entityData.parent_nodes);
        parentValidation.warnings.forEach(warning => {
          warnings.push({
            row: index + 1,
            field: warning.field,
            message: warning.message,
            type: 'suggestion'
          });
        });

        const domainValidation = validateDomainUrn(entityData.domain_urn);
        if (!domainValidation.isValid) {
          domainValidation.errors.forEach(error => {
            errors.push({
              row: index + 1,
              field: error.field,
              message: error.message,
              type: 'validation'
            });
          });
        }

        // Validate ownership columns
        const ownershipUsers = entityData.ownership_users || '';
        const ownershipGroups = entityData.ownership_groups || '';
        if (ownershipUsers || ownershipGroups) {
          // Basic validation for ownership format
          const userEntries = ownershipUsers.split('|').map(entry => entry.trim()).filter(entry => entry);
          const groupEntries = ownershipGroups.split('|').map(entry => entry.trim()).filter(entry => entry);
          
          // Check for valid ownership format
          [...userEntries, ...groupEntries].forEach(entry => {
            const parts = entry.split(':');
            if (parts.length < 2) {
              warnings.push({
                row: index + 1,
                field: 'ownership_users',
                message: `Invalid ownership format: "${entry}". Expected format: "owner:ownershipType"`,
                type: 'suggestion'
              });
            }
          });
        }

        const customPropsValidation = validateCustomProperties(entityData.custom_properties);
        customPropsValidation.warnings.forEach(warning => {
          warnings.push({
            row: index + 1,
            field: warning.field,
            message: warning.message,
            type: 'suggestion'
          });
        });

        const urlValidation = validateUrl(entityData.source_url);
        if (!urlValidation.isValid) {
          urlValidation.errors.forEach(error => {
            errors.push({
              row: index + 1,
              field: error.field,
              message: error.message,
              type: 'validation'
            });
          });
        }

        return entityData;
      });

      // Check for duplicate names
      const duplicateValidation = findDuplicateNames(data);
      if (!duplicateValidation.isValid) {
        duplicateValidation.errors.forEach(error => {
          errors.push({
            row: 0, // This is a cross-row validation
            field: error.field,
            message: error.message,
            type: 'duplicate'
          });
        });
      }

    } catch (error) {
      errors.push({
        row: 0,
        message: `Failed to parse CSV: ${error instanceof Error ? error.message : 'Unknown error'}`,
        type: 'format'
      });
    }

    return {
      data,
      errors,
      warnings
    };
  }, []);

  /**
   * Validate CSV data for errors and warnings
   */
  const validateCsvData = useCallback((data: EntityData[]): ValidationResult => {
    const errors: CsvError[] = [];
    const warnings: CsvWarning[] = [];

    if (data.length === 0) {
      errors.push({
        row: 0,
        message: 'CSV file is empty or contains no valid data',
        type: 'required'
      });
    }

    // Check for required headers
    const requiredHeaders = ['entity_type', 'name'];
    const sampleRow = data[0];
    if (sampleRow) {
      requiredHeaders.forEach(header => {
        if (!(header in sampleRow)) {
          errors.push({
            row: 0,
            field: header,
            message: `Required header "${header}" is missing`,
            type: 'required'
          });
        }
      });
    }

    // Validate each row
    data.forEach((entityData, index) => {
      const nameValidation = validateEntityName(entityData.name);
      if (!nameValidation.isValid) {
        nameValidation.errors.forEach(error => {
          errors.push({
            row: index + 1,
            field: error.field,
            message: error.message,
            type: 'validation'
          });
        });
      }

      const typeValidation = validateEntityType(entityData.entity_type);
      if (!typeValidation.isValid) {
        typeValidation.errors.forEach(error => {
          errors.push({
            row: index + 1,
            field: error.field,
            message: error.message,
            type: 'validation'
          });
        });
      }
    });

    // Check for duplicates
    const duplicateValidation = findDuplicateNames(data);
    if (!duplicateValidation.isValid) {
      duplicateValidation.errors.forEach(error => {
        errors.push({
          row: 0,
          field: error.field,
          message: error.message,
          type: 'duplicate'
        });
      });
    }

    return {
      isValid: errors.length === 0,
      errors: errors.map(error => ({
        field: error.field || 'general',
        message: error.message,
        code: error.type.toUpperCase()
      })),
      warnings: warnings.map(warning => ({
        field: warning.field || 'general',
        message: warning.message,
        code: warning.type.toUpperCase()
      }))
    };
  }, []);

  /**
   * Convert raw CSV row to EntityData format
   */
  const normalizeCsvRow = useCallback((row: any): EntityData => {
    return {
      entity_type: row.entity_type || '',
      urn: row.urn || '',
      name: row.name || '',
      description: row.description || '',
      term_source: row.term_source || '',
      source_ref: row.source_ref || '',
      source_url: row.source_url || '',
      ownership_users: row.ownership_users || '',
      ownership_groups: row.ownership_groups || '',
      parent_nodes: row.parent_nodes || '',
      related_contains: row.related_contains || '',
      related_inherits: row.related_inherits || '',
      domain_urn: row.domain_urn || '',
      domain_name: row.domain_name || '',
      custom_properties: row.custom_properties || '',
      status: row.status || ''
    };
  }, []);

  /**
   * Convert EntityData array back to CSV string
   */
  const toCsvString = useCallback((data: EntityData[]): string => {
    if (data.length === 0) return '';

    const headers = [
      'entity_type',
      'urn',
      'name',
      'description',
      'term_source',
      'source_ref',
      'source_url',
      'ownership_users',
      'ownership_groups',
      'parent_nodes',
      'related_contains',
      'related_inherits',
      'domain_urn',
      'domain_name',
      'custom_properties',
      'status'
    ];

    const csvData = data.map(entity => [
      entity.entity_type,
      entity.urn,
      entity.name,
      entity.description,
      entity.term_source,
      entity.source_ref,
      entity.source_url,
      entity.ownership_users,
      entity.ownership_groups,
      entity.parent_nodes,
      entity.related_contains,
      entity.related_inherits,
      entity.domain_urn,
      entity.domain_name,
      entity.custom_properties,
      entity.status
    ]);

    return Papa.unparse({
      fields: headers,
      data: csvData
    });
  }, []);

  /**
   * Create empty EntityData template
   */
  const createEmptyRow = useCallback((): EntityData => {
    return {
      entity_type: 'glossaryTerm',
      urn: '',
      name: '',
      description: '',
      term_source: '',
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
      status: ''
    };
  }, []);

  return {
    parseCsvText,
    validateCsvData,
    normalizeCsvRow,
    toCsvString,
    createEmptyRow
  };
}
