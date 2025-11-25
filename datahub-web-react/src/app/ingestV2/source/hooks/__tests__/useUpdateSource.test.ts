import { act, renderHook } from '@testing-library/react-hooks';
import { message } from 'antd';
import { Mock, beforeEach, describe, expect, it, vi } from 'vitest';

import { useExecuteIngestionSource } from '@app/ingestV2/source/hooks/useExecuteIngestionSource';
import { useUpdateIngestionSource } from '@app/ingestV2/source/hooks/useUpdateSource';
import { useOwnershipTypes } from '@app/sharedV2/owners/useOwnershipTypes';

import { useUpdateIngestionSourceMutation } from '@graphql/ingestion.generated';
import { useBatchAddOwnersMutation } from '@graphql/mutations.generated';
import { EntityType, UpdateIngestionSourceInput } from '@types';

// Mock all dependencies
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
        UpdateIngestionSourceEvent: 'UpdateIngestionSourceEvent',
    },
}));

describe('useUpdateIngestionSource', () => {
    const mockDefaultOwnershipType = { urn: 'urn:li:ownershipType:default' };
    const mockExecuteSource = vi.fn();
    const mockUpdateIngestionSource = vi.fn();
    const mockAddOwners = vi.fn();

    beforeEach(() => {
        vi.clearAllMocks();

        // Set up mock implementations
        (useExecuteIngestionSource as Mock).mockReturnValue(mockExecuteSource);
        (useOwnershipTypes as Mock).mockReturnValue({
            defaultOwnershipType: mockDefaultOwnershipType,
        });
        (useUpdateIngestionSourceMutation as Mock).mockReturnValue([mockUpdateIngestionSource]);
        (useBatchAddOwnersMutation as Mock).mockReturnValue([mockAddOwners]);
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

    it('should add owners when provided', async () => {
        const mockSourceUrn = 'urn:li:ingestionSource:test-source';
        const mockInput: UpdateIngestionSourceInput = {
            name: 'Updated Source',
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

        const mockUpdateResult = {
            data: {
                updateIngestionSource: true,
            },
        };

        mockUpdateIngestionSource.mockResolvedValue(mockUpdateResult);

        const { result } = renderHook(() => useUpdateIngestionSource());

        await act(async () => {
            await result.current(mockSourceUrn, mockInput, mockOwners);
        });

        // Verify update was called
        expect(mockUpdateIngestionSource).toHaveBeenCalledWith({
            variables: { urn: mockSourceUrn, input: mockInput },
        });

        // Verify addOwners was called with the correct parameters
        expect(mockAddOwners).toHaveBeenCalledWith({
            variables: {
                input: {
                    owners: [
                        {
                            ownerUrn: 'urn:li:corpuser:owner1',
                            ownerEntityType: EntityType.CorpUser,
                            ownershipTypeUrn: mockDefaultOwnershipType.urn,
                        },
                        {
                            ownerUrn: 'urn:li:corpuser:owner2',
                            ownerEntityType: EntityType.CorpUser,
                            ownershipTypeUrn: mockDefaultOwnershipType.urn,
                        },
                    ],
                    resources: [{ resourceUrn: mockSourceUrn }],
                },
            },
        });
    });

    it('should handle CorpGroup entity type for owners', async () => {
        const mockSourceUrn = 'urn:li:ingestionSource:test-source';
        const mockInput = {
            name: 'Updated Source',
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

        const mockUpdateResult = {
            data: {
                updateIngestionSource: true,
            },
        };

        mockUpdateIngestionSource.mockResolvedValue(mockUpdateResult);

        const { result } = renderHook(() => useUpdateIngestionSource());

        await act(async () => {
            await result.current(mockSourceUrn, mockInput, mockOwnerGroup);
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
                    resources: [{ resourceUrn: mockSourceUrn }],
                },
            },
        });
    });

    it('should trigger execution when shouldRun is true', async () => {
        const mockSourceUrn = 'urn:li:ingestionSource:test-source';
        const mockInput = {
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
            await result.current(mockSourceUrn, mockInput, undefined, true);
        });

        expect(mockExecuteSource).toHaveBeenCalledWith(mockSourceUrn);
    });

    it('should not trigger execution when shouldRun is false', async () => {
        const mockSourceUrn = 'urn:li:ingestionSource:test-source';
        const mockInput = {
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
            await result.current(mockSourceUrn, mockInput, undefined, false);
        });

        expect(mockExecuteSource).not.toHaveBeenCalled();
    });

    it('should show success message on successful update', async () => {
        const mockSourceUrn = 'urn:li:ingestionSource:test-source';
        const mockInput = {
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

        expect(message.success).toHaveBeenCalledWith({
            content: 'Successfully updated ingestion source!',
            duration: 3,
        });
    });

    it('should show error message on update failure', async () => {
        const mockSourceUrn = 'urn:li:ingestionSource:test-source';
        const mockInput = {
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

        await act(async () => {
            await result.current(mockSourceUrn, mockInput);
        });

        expect(message.destroy).toHaveBeenCalled();
        expect(message.error).toHaveBeenCalledWith({
            content: 'Failed to update ingestion source!: \n Update failed',
            duration: 3,
        });
    });

    it('should send analytics event on successful update', async () => {
        const mockSourceUrn = 'urn:li:ingestionSource:test-source';
        const mockInput = {
            name: 'Updated Source',
            type: 'test-type',
            schedule: { interval: '0 0 * * *', timezone: 'UTC' },
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

        // Mock analytics
        const { result } = renderHook(() => useUpdateIngestionSource());

        await act(async () => {
            await result.current(mockSourceUrn, mockInput);
        });

        // Get the actual analytics module to check the call
        const analytics = await import('@app/analytics');
        expect(analytics.default.event).toHaveBeenCalledWith({
            type: 'UpdateIngestionSourceEvent',
            sourceType: 'test-type',
            sourceUrn: mockSourceUrn,
            interval: '0 0 * * *',
            numOwners: undefined,
            outcome: 'save',
        });
    });

    it('should send analytics event with save_and_run outcome when shouldRun is true', async () => {
        const mockSourceUrn = 'urn:li:ingestionSource:test-source';
        const mockInput = {
            name: 'Updated Source',
            type: 'test-type',
            schedule: { interval: '0 0 * * *', timezone: 'utc' },
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

        // Mock analytics
        const { result } = renderHook(() => useUpdateIngestionSource());

        await act(async () => {
            await result.current(mockSourceUrn, mockInput, undefined, true);
        });

        // Get the actual analytics module to check the call
        const analytics = await import('@app/analytics');
        expect(analytics.default.event).toHaveBeenCalledWith({
            type: 'UpdateIngestionSourceEvent',
            sourceType: 'test-type',
            sourceUrn: mockSourceUrn,
            interval: '0 0 * * *',
            numOwners: undefined,
            outcome: 'save_and_run',
        });
    });

    it('should handle default ownership type being undefined', async () => {
        (useOwnershipTypes as Mock).mockReturnValue({
            defaultOwnershipType: undefined,
        });

        const mockSourceUrn = 'urn:li:ingestionSource:test-source';
        const mockInput = {
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
});
