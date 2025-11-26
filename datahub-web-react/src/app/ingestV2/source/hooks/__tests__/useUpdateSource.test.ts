import { act, renderHook } from '@testing-library/react-hooks';
import { message } from 'antd';
import { Mock, beforeEach, describe, expect, it, vi } from 'vitest';

import { useExecuteIngestionSource } from '@app/ingestV2/source/hooks/useExecuteIngestionSource';
import { useUpdateIngestionSource } from '@app/ingestV2/source/hooks/useUpdateSource';
import { useUpdateOwners } from '@app/sharedV2/owners/useUpdateOwners';

import { useUpdateIngestionSourceMutation } from '@graphql/ingestion.generated';
import { EntityType, UpdateIngestionSourceInput } from '@types';

// Mock all dependencies
vi.mock('@app/ingestV2/source/hooks/useExecuteIngestionSource');
vi.mock('@graphql/ingestion.generated');
vi.mock('@app/sharedV2/owners/useUpdateOwners');
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
    const mockExecuteSource = vi.fn();
    const mockUpdateIngestionSource = vi.fn();
    const mockUpdateOwners = vi.fn();

    beforeEach(() => {
        vi.clearAllMocks();

        // Set up mock implementations
        (useExecuteIngestionSource as Mock).mockReturnValue(mockExecuteSource);
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

        const mockExistingOwners = [
            {
                owner: {
                    urn: 'urn:li:corpuser:existing-owner1',
                    type: EntityType.CorpUser,
                    username: 'existing-owner1',
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

        const mockExistingOwners = [];

        const mockUpdateResult = {
            data: {
                updateIngestionSource: true,
            },
        };

        mockUpdateIngestionSource.mockResolvedValue(mockUpdateResult);

        const { result } = renderHook(() => useUpdateIngestionSource());

        await act(async () => {
            await result.current(mockSourceUrn, mockInput, mockOwnerGroup, mockExistingOwners);
        });

        expect(mockUpdateOwners).toHaveBeenCalledWith(mockOwnerGroup, mockExistingOwners, mockSourceUrn);
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
            await result.current(mockSourceUrn, mockInput, undefined, undefined, true);
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
            await result.current(mockSourceUrn, mockInput, undefined, undefined, false);
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

    it('should send analytics event with owners count when provided', async () => {
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

        const mockExistingOwners = [];

        const mockUpdateResult = {
            data: {
                updateIngestionSource: true,
            },
        };

        mockUpdateIngestionSource.mockResolvedValue(mockUpdateResult);

        // Mock analytics
        const { result } = renderHook(() => useUpdateIngestionSource());

        await act(async () => {
            await result.current(mockSourceUrn, mockInput, mockOwners, mockExistingOwners);
        });

        // Get the actual analytics module to check the call
        const analytics = await import('@app/analytics');
        expect(analytics.default.event).toHaveBeenCalledWith({
            type: 'UpdateIngestionSourceEvent',
            sourceType: 'test-type',
            sourceUrn: mockSourceUrn,
            interval: '0 0 * * *',
            numOwners: 2,
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
            await result.current(mockSourceUrn, mockInput, undefined, undefined, true);
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
});
