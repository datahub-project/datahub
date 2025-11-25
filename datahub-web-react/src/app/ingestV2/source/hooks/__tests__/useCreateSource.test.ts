import { act, renderHook } from '@testing-library/react-hooks';
import { message } from 'antd';
import { Mock, beforeEach, describe, expect, it, vi } from 'vitest';

import { useUserContext } from '@app/context/useUserContext';
import { useCreateSource } from '@app/ingestV2/source/hooks/useCreateSource';
import { useExecuteIngestionSource } from '@app/ingestV2/source/hooks/useExecuteIngestionSource';
import { useOwnershipTypes } from '@app/sharedV2/owners/useOwnershipTypes';

import { useCreateIngestionSourceMutation } from '@graphql/ingestion.generated';
import { useBatchAddOwnersMutation } from '@graphql/mutations.generated';
import { EntityType } from '@types';

// Mock all dependencies
vi.mock('@app/context/useUserContext');
vi.mock('@app/ingestV2/source/hooks/useExecuteIngestionSource');
vi.mock('@app/sharedV2/owners/useOwnershipTypes');
vi.mock('@graphql/ingestion.generated');
vi.mock('@graphql/mutations.generated');
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
    const mockUser = { urn: 'urn:li:corpuser:testuser' };
    const mockDefaultOwnershipType = { urn: 'urn:li:ownershipType:default' };
    const mockExecuteSource = vi.fn();
    const mockCreateIngestionSource = vi.fn();
    const mockAddOwners = vi.fn();

    beforeEach(() => {
        vi.clearAllMocks();

        // Set up mock implementations
        (useUserContext as Mock).mockReturnValue(mockUser);
        (useExecuteIngestionSource as Mock).mockReturnValue(mockExecuteSource);
        (useOwnershipTypes as Mock).mockReturnValue({
            defaultOwnershipType: mockDefaultOwnershipType,
        });
        (useCreateIngestionSourceMutation as Mock).mockReturnValue([mockCreateIngestionSource]);
        (useBatchAddOwnersMutation as Mock).mockReturnValue([mockAddOwners]);
    });

    it('should return a function', () => {
        const { result } = renderHook(() => useCreateSource());

        expect(typeof result.current).toBe('function');
    });

    it('should create ingestion source with basic input', async () => {
        const mockInput = {
            name: 'Test Source',
            type: 'test-type',
            config: {
                executorId: 'test',
                recipe: 'test',
            },
        };

        const mockCreateResult = {
            data: {
                createIngestionSource: 'urn:li:ingestionSource:test-source',
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
        const mockInput = {
            name: 'Test Source',
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
                urn: 'urn:li:corpuser:testuser',
                type: EntityType.CorpUser,
            },
        ];

        const mockCreateResult = {
            data: {
                createIngestionSource: 'urn:li:ingestionSource:test-source',
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

        // Verify addOwners was called with the correct parameters (excluding the current user)
        expect(mockAddOwners).toHaveBeenCalledWith({
            variables: {
                input: {
                    owners: [
                        {
                            ownerUrn: 'urn:li:corpuser:owner1',
                            ownerEntityType: 'CORP_USER',
                            ownershipTypeUrn: mockDefaultOwnershipType.urn,
                        },
                    ],
                    resources: [{ resourceUrn: 'urn:li:ingestionSource:test-source' }],
                },
            },
        });
    });

    it('should handle CorpGroup entity type for owners', async () => {
        const mockInput = {
            name: 'Test Source',
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
                createIngestionSource: 'urn:li:ingestionSource:test-source',
            },
        };

        mockCreateIngestionSource.mockResolvedValue(mockCreateResult);

        const { result } = renderHook(() => useCreateSource());

        await act(async () => {
            await result.current(mockInput, mockOwnerGroup);
        });

        expect(mockAddOwners).toHaveBeenCalledWith({
            variables: {
                input: {
                    owners: [
                        {
                            ownerUrn: 'urn:li:corpGroup:testgroup',
                            ownerEntityType: 'CORP_GROUP',
                            ownershipTypeUrn: mockDefaultOwnershipType.urn,
                        },
                    ],
                    resources: [{ resourceUrn: 'urn:li:ingestionSource:test-source' }],
                },
            },
        });
    });

    it('should trigger execution when shouldRun is true', async () => {
        const mockInput = {
            name: 'Test Source',
            type: 'test-type',
            config: {
                executorId: 'test',
                recipe: 'test',
            },
        };

        const mockCreateResult = {
            data: {
                createIngestionSource: 'urn:li:ingestionSource:test-source',
            },
        };

        mockCreateIngestionSource.mockResolvedValue(mockCreateResult);

        const { result } = renderHook(() => useCreateSource());

        await act(async () => {
            await result.current(mockInput, undefined, true);
        });

        expect(mockExecuteSource).toHaveBeenCalledWith('urn:li:ingestionSource:test-source');
    });

    it('should not trigger execution when shouldRun is false', async () => {
        const mockInput = {
            name: 'Test Source',
            type: 'test-type',
            config: {
                executorId: 'test',
                recipe: 'test',
            },
        };

        const mockCreateResult = {
            data: {
                createIngestionSource: 'urn:li:ingestionSource:test-source',
            },
        };

        mockCreateIngestionSource.mockResolvedValue(mockCreateResult);

        const { result } = renderHook(() => useCreateSource());

        await act(async () => {
            await result.current(mockInput, undefined, false);
        });

        expect(mockExecuteSource).not.toHaveBeenCalled();
    });

    it('should show success message on successful creation', async () => {
        const mockInput = {
            name: 'Test Source',
            type: 'test-type',
            config: {
                executorId: 'test',
                recipe: 'test',
            },
        };

        const mockCreateResult = {
            data: {
                createIngestionSource: 'urn:li:ingestionSource:test-source',
            },
        };

        mockCreateIngestionSource.mockResolvedValue(mockCreateResult);

        const { result } = renderHook(() => useCreateSource());

        await act(async () => {
            await result.current(mockInput);
        });

        expect(message.success).toHaveBeenCalledWith({
            content: 'Successfully created ingestion source!',
            duration: 3,
        });
    });

    it('should show error message on creation failure', async () => {
        const mockInput = {
            name: 'Test Source',
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

    it('should send analytics event on successful creation', async () => {
        const mockInput = {
            name: 'Test Source',
            type: 'test-type',
            schedule: { interval: '0 0 * * *', timezone: 'utc' },
            config: {
                executorId: 'test',
                recipe: 'test',
            },
        };

        const mockCreateResult = {
            data: {
                createIngestionSource: 'urn:li:ingestionSource:test-source',
            },
        };

        mockCreateIngestionSource.mockResolvedValue(mockCreateResult);

        // Mock analytics
        const { result } = renderHook(() => useCreateSource());

        await act(async () => {
            await result.current(mockInput);
        });

        // Get the actual analytics module to check the call
        const analytics = await import('@app/analytics');
        expect(analytics.default.event).toHaveBeenCalledWith({
            type: 'CreateIngestionSourceEvent',
            sourceType: 'test-type',
            sourceUrn: 'urn:li:ingestionSource:test-source',
            interval: '0 0 * * *',
            numOwners: undefined,
            outcome: 'save',
        });
    });

    it('should send analytics event with save_and_run outcome when shouldRun is true', async () => {
        const mockInput = {
            name: 'Test Source',
            type: 'test-type',
            schedule: { interval: '0 0 * * *', timezone: 'utc' },
            config: {
                executorId: 'test',
                recipe: 'test',
            },
        };

        const mockCreateResult = {
            data: {
                createIngestionSource: 'urn:li:ingestionSource:test-source',
            },
        };

        mockCreateIngestionSource.mockResolvedValue(mockCreateResult);

        const { result } = renderHook(() => useCreateSource());

        await act(async () => {
            await result.current(mockInput, undefined, true);
        });

        // Get the actual analytics module to check the call
        const analytics = await import('@app/analytics');
        expect(analytics.default.event).toHaveBeenCalledWith({
            type: 'CreateIngestionSourceEvent',
            sourceType: 'test-type',
            sourceUrn: 'urn:li:ingestionSource:test-source',
            interval: '0 0 * * *',
            numOwners: undefined,
            outcome: 'save_and_run',
        });
    });

    it('should handle default ownership type being undefined', async () => {
        (useOwnershipTypes as Mock).mockReturnValue({
            defaultOwnershipType: undefined,
        });

        const mockInput = {
            name: 'Test Source',
            type: 'test-type',
            config: {
                executorId: 'test',
                recipe: 'test',
            },
        };

        const mockCreateResult = {
            data: {
                createIngestionSource: 'urn:li:ingestionSource:test-source',
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
});
