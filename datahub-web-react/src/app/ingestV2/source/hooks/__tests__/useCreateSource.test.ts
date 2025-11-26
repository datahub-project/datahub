import { act, renderHook } from '@testing-library/react-hooks';
import { message } from 'antd';
import { Mock, beforeEach, describe, expect, it, vi } from 'vitest';

import { useCreateSource } from '@app/ingestV2/source/hooks/useCreateSource';
import { useExecuteIngestionSource } from '@app/ingestV2/source/hooks/useExecuteIngestionSource';
import { useAddOwners } from '@app/sharedV2/owners/useAddOwners';
import { useOwnershipTypes } from '@app/sharedV2/owners/useOwnershipTypes';

import { useCreateIngestionSourceMutation } from '@graphql/ingestion.generated';
import { EntityType, UpdateIngestionSourceInput } from '@types';

// Mock all dependencies
vi.mock('@app/ingestV2/source/hooks/useExecuteIngestionSource');
vi.mock('@graphql/ingestion.generated');
vi.mock('@app/sharedV2/owners/useAddOwners');
vi.mock('@app/sharedV2/owners/useOwnershipTypes');
vi.mock('antd', async () => {
    const actual = await vi.importActual('antd');
    return {
        ...actual,
        message: {
            loading: vi.fn(),
            success: vi.fn(),
            error: vi.fn(),
            destroy: vi.fn(),
        },
    };
});

vi.mock('@app/analytics', () => ({
    default: {
        event: vi.fn(),
    },
    EventType: {
        CreateIngestionSourceEvent: 'CreateIngestionSourceEvent',
    },
}));

describe('useCreateSource', () => {
    const mockExecuteSource = vi.fn();
    const mockCreateIngestionSource = vi.fn();
    const mockAddOwners = vi.fn();
    const mockDefaultOwnershipType = 'OWNER';

    beforeEach(() => {
        vi.clearAllMocks();

        // Set up mock implementations
        (useExecuteIngestionSource as Mock).mockReturnValue(mockExecuteSource);
        (useCreateIngestionSourceMutation as Mock).mockReturnValue([mockCreateIngestionSource]);
        (useAddOwners as Mock).mockReturnValue(mockAddOwners);
        (useOwnershipTypes as Mock).mockReturnValue({ defaultOwnershipType: mockDefaultOwnershipType });
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
            await result.current(mockInput);
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

        const mockOwners = [
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

    it('should handle CorpGroup entity type for owners', async () => {
        const mockInput: UpdateIngestionSourceInput = {
            name: 'New Source',
            type: 'test-type',
            config: {
                executorId: 'test',
                recipe: 'test',
            },
        };

        const mockOwnerGroup = [
            {
                urn: 'urn:li:corpGroup:testgroup',
                type: EntityType.CorpGroup,
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
            await result.current(mockInput, mockOwnerGroup);
        });

        expect(mockAddOwners).toHaveBeenCalledWith(mockOwnerGroup, 'urn:li:ingestionSource:new-source');
    });

    it('should trigger execution when shouldRun is true', async () => {
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
            await result.current(mockInput, undefined, true);
        });

        expect(mockExecuteSource).toHaveBeenCalledWith('urn:li:ingestionSource:new-source');
    });

    it('should not trigger execution when shouldRun is false', async () => {
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
            await result.current(mockInput, undefined, false);
        });

        expect(mockExecuteSource).not.toHaveBeenCalled();
    });

    it('should show loading and success messages on successful creation', async () => {
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
            await result.current(mockInput);
        });

        expect(message.loading).toHaveBeenCalledWith({
            content: 'Loading...',
            duration: 2,
        });
        expect(message.success).toHaveBeenCalledWith({
            content: 'Successfully created ingestion source!',
            duration: 3,
        });
    });

    it('should show error message on creation failure', async () => {
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

        await act(async () => {
            await result.current(mockInput);
        });

        expect(message.destroy).toHaveBeenCalled();
        expect(message.error).toHaveBeenCalledWith({
            content: 'Failed to create ingestion source!: \n Creation failed',
            duration: 3,
        });
    });

    it('should use placeholder URN when creation result is null', async () => {
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

        await act(async () => {
            await result.current(mockInput);
        });

        // Verify that addOwners is called with the placeholder URN and undefined owners
        expect(mockAddOwners).toHaveBeenCalledWith(undefined, 'placeholder-urn');
    });
});
