import { useCallback } from 'react';
import { Entity, ComparisonResult, UseEntityComparisonReturn } from '../../glossary.types';
import { mockComparisonResult } from '../mocks/mockData';

export function useMockEntityComparison(): UseEntityComparisonReturn {
  const categorizeEntities = useCallback((entities: Entity[], existingEntities: Entity[]): ComparisonResult => {
    // Return mock comparison result
    return mockComparisonResult;
  }, []);

  const detectConflicts = useCallback((entity1: Entity, entity2: Entity): string[] => {
    // Mock conflict detection
    const conflicts: string[] = [];
    
    if (entity1.data.description !== entity2.data.description) {
      conflicts.push('description');
    }
    
    if (entity1.data.ownership_users !== entity2.data.ownership_users) {
      conflicts.push('ownership_users');
    }
    
    if (entity1.data.ownership_groups !== entity2.data.ownership_groups) {
      conflicts.push('ownership_groups');
    }
    
    return conflicts;
  }, []);

  const getChangeDetails = useCallback((entity1: Entity, entity2: Entity): Record<string, { old: any; new: any }> => {
    // Mock change details
    const changes: Record<string, { old: any; new: any }> = {};
    
    if (entity1.data.description !== entity2.data.description) {
      changes.description = {
        old: entity1.data.description,
        new: entity2.data.description
      };
    }
    
    if (entity1.data.ownership_users !== entity2.data.ownership_users) {
      changes.ownership_users = {
        old: entity1.data.ownership_users,
        new: entity2.data.ownership_users
      };
    }
    
    return changes;
  }, []);

  const compareEntityData = useCallback((data1: any, data2: any): boolean => {
    // Mock data comparison
    return data1.name === data2.name && 
           data1.description === data2.description &&
           data1.ownership_users === data2.ownership_users &&
           data1.ownership_groups === data2.ownership_groups;
  }, []);

  const identifyChanges = useCallback((data1: any, data2: any): string[] => {
    // Mock change identification
    const changes: string[] = [];
    
    if (data1.description !== data2.description) {
      changes.push('description');
    }
    
    if (data1.ownership_users !== data2.ownership_users) {
      changes.push('ownership_users');
    }
    
    if (data1.ownership_groups !== data2.ownership_groups) {
      changes.push('ownership_groups');
    }
    
    return changes;
  }, []);

  return {
    categorizeEntities,
    detectConflicts,
    getChangeDetails,
    compareEntityData,
    identifyChanges
  };
}
