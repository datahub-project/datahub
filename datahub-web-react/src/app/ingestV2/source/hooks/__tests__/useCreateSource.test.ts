import { act, renderHook } from '@testing-library/react-hooks';
import { Mock, beforeEach, describe, expect, it, vi } from 'vitest';

import { useCreateSource } from '@app/ingestV2/source/hooks/useCreateSource';
import { useAddOwners } from '@app/sharedV2/owners/useAddOwners';

import { useCreateIngestionSourceMutation } from '@graphql/ingestion.generated';
import { Entity, EntityType, UpdateIngestionSourceInput } from '@types';

// Mock all dependencies
vi.mock('@graphql/ingestion.generated');
vi.mock('@app/sharedV2/owners/useAddOwners');

describe('useCreateSource', () => {
    const mockCreateIngestionSource = vi.fn();
    const mockAddOwners = vi.fn();

    beforeEach(() => {
        vi.clearAllMocks();

        // Set up mock implementations
        (useCreateIngestionSourceMutation as Mock).mockReturnValue([mockCreateIngestionSource]);
        (useAddOwners as Mock).mockReturnValue(mockAddOwners);
    });

    it('should return a function', () => {
        const { result } = renderHook(() => useCreateSource());

        expect(typeof result.current).toBe('function');
    });

    it('should create ingestion source with basic input', async () => {
        const mockInput: UpdateIngestionSourceInput = {
            name: 'New Source',
            type: 'test-type',
            config: {
                executorId: 'test',
                recipe: 'test',
            },
        };

        const mockCreateResult = {
            data: {
                createIngestionSource: 'urn:li:ingestionSource:new-source',
            },
        };

        mockCreateIngestionSource.mockResolvedValue(mockCreateResult);

        const { result } = renderHook(() => useCreateSource());

        await act(async () => {
            const urn = await result.current(mockInput);
            expect(urn).toBe('urn:li:ingestionSource:new-source');
        });

        expect(mockCreateIngestionSource).toHaveBeenCalledWith({
            variables: { input: mockInput },
        });
    });

    it('should add owners when provided', async () => {
        const mockInput: UpdateIngestionSourceInput = {
            name: 'New Source',
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

        const mockCreateResult = {
            data: {
                createIngestionSource: 'urn:li:ingestionSource:new-source',
            },
        };

        mockCreateIngestionSource.mockResolvedValue(mockCreateResult);

        const { result } = renderHook(() => useCreateSource());

        await act(async () => {
            await result.current(mockInput, mockOwners);
        });

        // Verify create was called
        expect(mockCreateIngestionSource).toHaveBeenCalledWith({
            variables: { input: mockInput },
        });

        // Verify addOwners was called with the correct parameters
        expect(mockAddOwners).toHaveBeenCalledWith(mockOwners, 'urn:li:ingestionSource:new-source');
    });

    it('should resolve with undefined when source creation fails', async () => {
        const mockInput: UpdateIngestionSourceInput = {
            name: 'New Source',
            type: 'test-type',
            config: {
                executorId: 'test',
                recipe: 'test',
            },
        };

        const mockCreateResult = {
            data: {
                createIngestionSource: null,
            },
        };

        mockCreateIngestionSource.mockResolvedValue(mockCreateResult);

        const { result } = renderHook(() => useCreateSource());

        await expect(
            act(async () => {
                const urn = await result.current(mockInput);
                expect(urn).toBeUndefined();
            }),
        ).rejects.toThrow('Failed to create ingestion source!');
    });

    it('should reject with error when mutation fails', async () => {
        const mockInput: UpdateIngestionSourceInput = {
            name: 'New Source',
            type: 'test-type',
            config: {
                executorId: 'test',
                recipe: 'test',
            },
        };

        const mockError = new Error('Creation failed');
        mockCreateIngestionSource.mockRejectedValue(mockError);

        const { result } = renderHook(() => useCreateSource());

        await expect(
            act(async () => {
                await result.current(mockInput);
            }),
        ).rejects.toThrow('Failed to create ingestion source!: \n Creation failed');
    });
});
