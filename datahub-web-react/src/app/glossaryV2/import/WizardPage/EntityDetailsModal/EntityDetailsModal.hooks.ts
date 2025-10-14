import { useState, useCallback, useMemo } from 'react';
import { EntityData, ValidationError, ValidationWarning } from '../../glossary.types';

interface UseEntityDetailsProps {
  originalData: EntityData | null;
  editedData: EntityData | null;
}

interface UseEntityDetailsReturn {
  validationErrors: ValidationError[];
  validationWarnings: ValidationWarning[];
  hasChanges: boolean;
  resetChanges: () => void;
  validateField: (field: keyof EntityData, value: string) => ValidationError[];
  validateAllFields: () => boolean;
}

export const useEntityDetails = (
  originalData: EntityData | null,
  editedData: EntityData | null
): UseEntityDetailsReturn => {
  const [validationErrors, setValidationErrors] = useState<ValidationError[]>([]);
  const [validationWarnings, setValidationWarnings] = useState<ValidationWarning[]>([]);

  const hasChanges = useMemo(() => {
    if (!originalData || !editedData) return false;
    
    return Object.keys(originalData).some(key => {
      const field = key as keyof EntityData;
      return originalData[field] !== editedData[field];
    });
  }, [originalData, editedData]);

  const validateField = useCallback((field: keyof EntityData, value: string): ValidationError[] => {
    const errors: ValidationError[] = [];

    // Required field validation
    if (field === 'name' && !value.trim()) {
      errors.push({
        field: 'name',
        message: 'Name is required',
        code: 'REQUIRED'
      });
    }

    if (field === 'entity_type' && !value.trim()) {
      errors.push({
        field: 'entity_type',
        message: 'Entity type is required',
        code: 'REQUIRED'
      });
    }

    // Format validation
    if (field === 'entity_type' && value && !['glossaryTerm', 'glossaryNode'].includes(value)) {
      errors.push({
        field: 'entity_type',
        message: 'Entity type must be either "glossaryTerm" or "glossaryNode"',
        code: 'INVALID_FORMAT'
      });
    }

    // URL validation
    if (field === 'source_url' && value && !isValidUrl(value)) {
      errors.push({
        field: 'source_url',
        message: 'Source URL must be a valid URL',
        code: 'INVALID_FORMAT'
      });
    }

    // URN validation
    if (field === 'urn' && value && !isValidUrn(value)) {
      errors.push({
        field: 'urn',
        message: 'URN must be a valid DataHub URN',
        code: 'INVALID_FORMAT'
      });
    }

    return errors;
  }, []);

  const validateAllFields = useCallback((): boolean => {
    if (!editedData) return false;

    const allErrors: ValidationError[] = [];
    const allWarnings: ValidationWarning[] = [];

    // Validate each field
    Object.keys(editedData).forEach(key => {
      const field = key as keyof EntityData;
      const value = editedData[field] || '';
      const fieldErrors = validateField(field, value);
      allErrors.push(...fieldErrors);
    });

    // Cross-field validation
    if (editedData.entity_type === 'glossaryTerm' && !editedData.parent_nodes) {
      allWarnings.push({
        field: 'parent_nodes',
        message: 'Terms should typically have a parent node',
        code: 'RECOMMENDATION'
      });
    }

    // Hierarchy validation
    if (editedData.parent_nodes) {
      const parentNames = editedData.parent_nodes.split(',').map(name => name.trim());
      parentNames.forEach(parentName => {
        if (parentName && parentName === editedData.name) {
          allErrors.push({
            field: 'parent_nodes',
            message: 'Entity cannot be its own parent',
            code: 'INVALID_HIERARCHY'
          });
        }
      });
    }

    setValidationErrors(allErrors);
    setValidationWarnings(allWarnings);

    return allErrors.length === 0;
  }, [editedData, validateField]);

  const resetChanges = useCallback(() => {
    setValidationErrors([]);
    setValidationWarnings([]);
  }, []);

  return {
    validationErrors,
    validationWarnings,
    hasChanges,
    resetChanges,
    validateField,
    validateAllFields,
  };
};

// Helper functions
function isValidUrl(url: string): boolean {
  try {
    new URL(url);
    return true;
  } catch {
    return false;
  }
}

function isValidUrn(urn: string): boolean {
  // Basic URN validation - should start with 'urn:li:'
  return urn.startsWith('urn:li:') && urn.length > 10;
}
