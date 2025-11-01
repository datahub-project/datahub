/**
 * Tests for useComprehensiveImport Hook
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { ApolloClient } from '@apollo/client';
import { useComprehensiveImport } from '../useComprehensiveImport';
import { Entity } from '../../../glossary.types';

// Mock Apollo Client
const mockApolloClient = {
  mutate: vi.fn(),
  query: vi.fn()
} as unknown as ApolloClient<any>;

// Mock GraphQL operations
const mockExecutePatchEntitiesMutation = vi.fn();
const mockExecuteGetOwnershipTypesQuery = vi.fn();

// Mock other hooks
vi.mock('../useGraphQLOperations', () => ({
  useGraphQLOperations: () => ({
    executePatchEntitiesMutation: mockExecutePatchEntitiesMutation,
    executeGetOwnershipTypesQuery: mockExecuteGetOwnershipTypesQuery
  })
}));

vi.mock('../useHierarchyManagement', () => ({
  useHierarchyManagement: () => ({
    validateHierarchy: vi.fn(() => ({ isValid: true, errors: [], warnings: [] }))
  })
}));

vi.mock('../useEntityManagement', () => ({
  useEntityManagement: () => ({
    normalizeCsvData: vi.fn(),
    compareEntities: vi.fn()
  })
}));

vi.mock('../useEntityComparison', () => ({
  useEntityComparison: () => ({
    categorizeEntities: vi.fn((entities) => ({
      newEntities: entities.filter(e => e.status === 'new'),
      updatedEntities: entities.filter(e => e.status === 'updated'),
      unchangedEntities: entities.filter(e => e.status === 'existing'),
      conflictedEntities: entities.filter(e => e.status === 'conflict')
    }))
  })
}));

vi.mock('@app/context/useUserContext', () => ({
  useUserContext: () => ({
    user: { urn: 'urn:li:corpuser:testuser' }
  })
}));

describe('useComprehensiveImport', () => {
  const mockEntities: Entity[] = [
    {
      id: '1',
      name: 'Test Term',
      type: 'glossaryTerm',
      status: 'new',
      parentNames: [],
      parentUrns: [],
      level: 0,
      data: {
        entity_type: 'glossaryTerm',
        urn: '',
        name: 'Test Term',
        description: 'Test Description',
        term_source: 'INTERNAL',
        source_ref: '',
        source_url: '',
        ownership_users: 'admin:DEVELOPER',
        ownership_groups: '',
        parent_nodes: '',
        related_contains: '',
        related_inherits: '',
        domain_urn: '',
        domain_name: '',
        custom_properties: ''
      }
    }
  ];

  const mockExistingEntities: Entity[] = [
    {
      id: 'existing-1',
      name: 'Existing Term',
      type: 'glossaryTerm',
      status: 'existing',
      urn: 'urn:li:glossaryTerm:existing',
      parentNames: [],
      parentUrns: [],
      level: 0,
      data: {
        entity_type: 'glossaryTerm',
        urn: 'urn:li:glossaryTerm:existing',
        name: 'Existing Term',
        description: 'Existing Description',
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
        custom_properties: ''
      }
    }
  ];

  beforeEach(() => {
    vi.clearAllMocks();
    
    // Mock successful ownership types query
    mockExecuteGetOwnershipTypesQuery.mockResolvedValue({
      data: {
        listOwnershipTypes: {
          ownershipTypes: [
            {
              urn: 'urn:li:ownershipType:developer',
              info: { name: 'DEVELOPER' }
            }
          ]
        }
      }
    });

    // Mock successful patch entities mutation
    mockExecutePatchEntitiesMutation.mockResolvedValue([
      { urn: 'urn:li:glossaryTerm:generated', success: true, name: 'Test Term' }
    ]);
  });

  describe('initial state', () => {
    it('should initialize with correct default state', () => {
      const { result } = renderHook(() => useComprehensiveImport({
        apolloClient: mockApolloClient
      }));

      expect(result.current.progress).toEqual({
        total: 0,
        processed: 0,
        successful: 0,
        failed: 0,
        errors: [],
        warnings: []
      });
      expect(result.current.isProcessing).toBe(false);
      expect(result.current.isPaused).toBe(false);
      expect(result.current.isCancelled).toBe(false);
    });
  });

  describe('startImport', () => {
    it('should successfully import entities', async () => {
      const { result } = renderHook(() => useComprehensiveImport({
        apolloClient: mockApolloClient
      }));

      await act(async () => {
        await result.current.startImport(mockEntities, mockExistingEntities);
      });

      expect(mockExecuteGetOwnershipTypesQuery).toHaveBeenCalled();
      expect(mockExecutePatchEntitiesMutation).toHaveBeenCalled();
      expect(result.current.progress.successful).toBeGreaterThan(0);
      expect(result.current.progress.failed).toBe(0);
    });

    it('should handle import failure', async () => {
      mockExecutePatchEntitiesMutation.mockRejectedValue(new Error('Import failed'));

      const { result } = renderHook(() => useComprehensiveImport({
        apolloClient: mockApolloClient
      }));

      await act(async () => {
        await result.current.startImport(mockEntities, mockExistingEntities);
      });

      expect(result.current.progress.errors).toHaveLength(1);
      expect(result.current.progress.errors[0].error).toContain('Import failed');
      expect(result.current.progress.failed).toBeGreaterThan(0);
    });

    it('should handle no entities to process', async () => {
      const { result } = renderHook(() => useComprehensiveImport({
        apolloClient: mockApolloClient
      }));

      await act(async () => {
        await result.current.startImport([], mockExistingEntities);
      });

      expect(mockExecutePatchEntitiesMutation).not.toHaveBeenCalled();
      expect(result.current.progress.currentPhase).toBe('No entities to process');
    });

    it('should handle hierarchy validation failure', async () => {
      const { useHierarchyManagement } = await import('../useHierarchyManagement');
      const mockValidateHierarchy = vi.fn(() => ({
        isValid: false,
        errors: [{ field: 'hierarchy', message: 'Circular dependency detected' }],
        warnings: []
      }));

      vi.mocked(useHierarchyManagement).mockReturnValue({
        validateHierarchy: mockValidateHierarchy,
        createProcessingOrder: vi.fn(),
        resolveParentUrns: vi.fn(),
        resolveParentUrnsForLevel: vi.fn()
      });

      const { result } = renderHook(() => useComprehensiveImport({
        apolloClient: mockApolloClient
      }));

      await act(async () => {
        await result.current.startImport(mockEntities, mockExistingEntities);
      });

      expect(result.current.progress.errors).toHaveLength(1);
      expect(result.current.progress.errors[0].error).toBe('Circular dependency detected');
      expect(mockExecutePatchEntitiesMutation).not.toHaveBeenCalled();
    });
  });

  describe('pause and resume', () => {
    it('should pause import', () => {
      const { result } = renderHook(() => useComprehensiveImport({
        apolloClient: mockApolloClient
      }));

      act(() => {
        result.current.pauseImport();
      });

      expect(result.current.isPaused).toBe(true);
      expect(result.current.progress.currentPhase).toBe('Import paused');
    });

    it('should resume import', () => {
      const { result } = renderHook(() => useComprehensiveImport({
        apolloClient: mockApolloClient
      }));

      act(() => {
        result.current.pauseImport();
        result.current.resumeImport();
      });

      expect(result.current.isPaused).toBe(false);
      expect(result.current.progress.currentPhase).toBe('Resuming import...');
    });
  });

  describe('cancel import', () => {
    it('should cancel import', () => {
      const { result } = renderHook(() => useComprehensiveImport({
        apolloClient: mockApolloClient
      }));

      act(() => {
        result.current.cancelImport();
      });

      expect(result.current.isCancelled).toBe(true);
      expect(result.current.isProcessing).toBe(false);
      expect(result.current.isPaused).toBe(false);
      expect(result.current.progress.currentPhase).toBe('Import cancelled');
    });
  });

  describe('retry failed', () => {
    it('should handle retry with no failed operations', () => {
      const { result } = renderHook(() => useComprehensiveImport({
        apolloClient: mockApolloClient
      }));

      act(() => {
        result.current.retryFailed();
      });

      // Should not throw error
      expect(result.current.isProcessing).toBe(false);
    });

    it('should handle retry with failed operations', async () => {
      const { result } = renderHook(() => useComprehensiveImport({
        apolloClient: mockApolloClient
      }));

      // First fail an import
      mockExecutePatchEntitiesMutation.mockRejectedValue(new Error('Import failed'));

      await act(async () => {
        await result.current.startImport(mockEntities, mockExistingEntities);
      });

      expect(result.current.progress.errors.length).toBeGreaterThan(0);

      // Then try to retry
      act(() => {
        result.current.retryFailed();
      });

      // Should reset progress
      expect(result.current.progress.errors).toHaveLength(0);
    });
  });

  describe('reset progress', () => {
    it('should reset all progress state', () => {
      const { result } = renderHook(() => useComprehensiveImport({
        apolloClient: mockApolloClient
      }));

      // Set some state
      act(() => {
        result.current.pauseImport();
      });

      expect(result.current.isPaused).toBe(true);

      // Reset
      act(() => {
        result.current.resetProgress();
      });

      expect(result.current.progress).toEqual({
        total: 0,
        processed: 0,
        successful: 0,
        failed: 0,
        errors: [],
        warnings: []
      });
      expect(result.current.isProcessing).toBe(false);
      expect(result.current.isPaused).toBe(false);
      expect(result.current.isCancelled).toBe(false);
    });
  });

  describe('progress tracking', () => {
    it('should call onProgress callback when provided', async () => {
      const onProgress = vi.fn();

      const { result } = renderHook(() => useComprehensiveImport({
        apolloClient: mockApolloClient,
        onProgress
      }));

      await act(async () => {
        await result.current.startImport(mockEntities, mockExistingEntities);
      });

      expect(onProgress).toHaveBeenCalled();
    });
  });
});
