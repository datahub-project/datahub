import { useCallback } from 'react';
import { 
  EntityData, 
  CsvParseResult, 
  CsvError, 
  CsvWarning, 
  ValidationResult,
  UseCsvProcessingReturn 
} from '../../glossary.types';
import { mockCsvData } from '../mocks/mockData';

export function useMockCsvProcessing(): UseCsvProcessingReturn {
  const parseCsvText = useCallback((csvText: string): CsvParseResult => {
    // Return mock parsed data
    return {
      data: mockCsvData,
      errors: [],
      warnings: [],
      totalRows: mockCsvData.length,
      validRows: mockCsvData.length
    };
  }, []);

  const validateCsvData = useCallback((data: EntityData[]): ValidationResult => {
    // Return mock validation result
    return {
      isValid: true,
      errors: [],
      warnings: [
        {
          row: 2,
          field: 'ownership_users',
          message: 'Auto-discovered 1 parent relationship(s) from existing entities',
          type: 'suggestion'
        }
      ]
    };
  }, []);

  const normalizeCsvRow = useCallback((row: any): EntityData => {
    // Return mock normalized data
    return {
      entity_type: row.entity_type || 'glossaryTerm',
      urn: row.urn || '',
      name: row.name || '',
      description: row.description || '',
      term_source: row.term_source || 'INTERNAL',
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

  const toCsvString = useCallback((data: EntityData[]): string => {
    // Return mock CSV string
    const headers = [
      'entity_type', 'urn', 'name', 'description', 'term_source', 'source_ref', 'source_url',
      'ownership_users', 'ownership_groups', 'parent_nodes', 'related_contains', 'related_inherits',
      'domain_urn', 'domain_name', 'custom_properties', 'status'
    ];
    
    const csvRows = data.map(entity => [
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
    
    return [headers, ...csvRows].map(row => row.join(',')).join('\n');
  }, []);

  const createEmptyRow = useCallback((): EntityData => {
    return {
      entity_type: 'glossaryTerm',
      urn: '',
      name: '',
      description: '',
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
