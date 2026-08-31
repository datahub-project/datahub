import { act, renderHook } from '@testing-library/react-hooks';
import { Mock, beforeEach, describe, expect, it, vi } from 'vitest';

import { useUpdateIngestionSource } from '@app/ingestV2/source/hooks/useUpdateSource';
import { useUpdateOwners } from '@app/sharedV2/owners/useUpdateOwners';

import { useUpdateIngestionSourceMutation } from '@graphql/ingestion.generated';
import { Entity, EntityType, Owner, UpdateIngestionSourceInput } from '@types';

// Mock all dependencies
vi.mock('@graphql/ingestion.generated');
vi.mock('@app/sharedV2/owners/useUpdateOwners');

describe('useUpdateIngestionSource', () => {
    const mockUpdateIngestionSource = vi.fn();
    const mockUpdateOwners = vi.fn();

    beforeEach(() => {
        vi.clearAllMocks();

        // Set up mock implementations
        (useUpdateIngestionSourceMutation as Mock).mockReturnValue([mockUpdateIngestionSource]);
        (useUpdateOwners as Mock).mockReturnValue(mockUpdateOwners);
    });

    it('should return a function', () => {
        const { result } = renderHook(() => useUpdateIngestionSource());

        expect(typeof result.current).toBe('function');
    });

    it('should update ingestion source with basic input', async () => {
        const mockSourceUrn = 'urn:li:ingestionSource:test-source';
        const mockInput: UpdateIngestionSourceInput = {
            name: 'Updated Source',
            type: 'test-type',
            config: {
                executorId: 'test',
                recipe: 'test',
            },
        };

        const mockUpdateResult = {
            data: {
                updateIngestionSource: true,
            },
        };

        mockUpdateIngestionSource.mockResolvedValue(mockUpdateResult);

        const { result } = renderHook(() => useUpdateIngestionSource());

        await act(async () => {
            await result.current(mockSourceUrn, mockInput);
        });

        expect(mockUpdateIngestionSource).toHaveBeenCalledWith({
            variables: { urn: mockSourceUrn, input: mockInput },
        });
    });

    it('should update owners when provided', async () => {
        const mockSourceUrn = 'urn:li:ingestionSource:test-source';
        const mockInput: UpdateIngestionSourceInput = {
            name: 'Updated Source',
            type: 'test-type',
            config: {
                executorId: 'test',
                recipe: 'test',
            },
        };

        const mockOwners: Entity[] = [
            {
                urn: 'urn:li:corpuser:owner1',
                type: EntityType.CorpUser,
            },
            {
                urn: 'urn:li:corpuser:owner2',
                type: EntityType.CorpUser,
            },
        ];

        const mockExistingOwners: Owner[] = [
            {
                owner: {
                    urn: 'urn:li:corpuser:existing-owner1',
                    username: 'existing-owner1',
                    type: EntityType.CorpUser,
                },
                associatedUrn: mockSourceUrn,
            },
        ];

        const mockUpdateResult = {
            data: {
                updateIngestionSource: true,
            },
        };

        mockUpdateIngestionSource.mockResolvedValue(mockUpdateResult);

        const { result } = renderHook(() => useUpdateIngestionSource());

        await act(async () => {
            await result.current(mockSourceUrn, mockInput, mockOwners, mockExistingOwners);
        });

        // Verify update was called
        expect(mockUpdateIngestionSource).toHaveBeenCalledWith({
            variables: { urn: mockSourceUrn, input: mockInput },
        });

        // Verify updateOwners was called with the correct parameters
        expect(mockUpdateOwners).toHaveBeenCalledWith(mockOwners, mockExistingOwners, mockSourceUrn);
    });

    it('should resolve when update is successful', async () => {
        const mockSourceUrn = 'urn:li:ingestionSource:test-source';
        const mockInput: UpdateIngestionSourceInput = {
            name: 'Updated Source',
            type: 'test-type',
            config: {
                executorId: 'test',
                recipe: 'test',
            },
        };

        const mockUpdateResult = {
            data: {
                updateIngestionSource: true,
            },
        };

        mockUpdateIngestionSource.mockResolvedValue(mockUpdateResult);

        const { result } = renderHook(() => useUpdateIngestionSource());

        await act(async () => {
            await expect(result.current(mockSourceUrn, mockInput)).resolves.not.toThrow();
        });
    });

    it('should reject with error when mutation fails', async () => {
        const mockSourceUrn = 'urn:li:ingestionSource:test-source';
        const mockInput: UpdateIngestionSourceInput = {
            name: 'Updated Source',
            type: 'test-type',
            config: {
                executorId: 'test',
                recipe: 'test',
            },
        };

        const mockError = new Error('Update failed');
        mockUpdateIngestionSource.mockRejectedValue(mockError);

        const { result } = renderHook(() => useUpdateIngestionSource());

        await expect(
            act(async () => {
                await result.current(mockSourceUrn, mockInput);
            }),
        ).rejects.toThrow('Failed to update ingestion source!: \n Update failed');
    });
});
