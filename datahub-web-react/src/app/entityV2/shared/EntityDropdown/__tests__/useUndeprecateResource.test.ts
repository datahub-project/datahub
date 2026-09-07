import { toast } from '@components';
import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import analytics, { EventType } from '@app/analytics';
import { useUndeprecateResource } from '@app/entityV2/shared/EntityDropdown/useUndeprecateResource';

import { SubResourceType } from '@types';

const batchUpdateDeprecationMock = vi.fn();
vi.mock('@graphql/mutations.generated', () => ({
    useBatchUpdateDeprecationMutation: () => [batchUpdateDeprecationMock],
}));

vi.mock('@components', () => ({
    toast: {
        success: vi.fn(),
        error: vi.fn(),
        destroy: vi.fn(),
    },
}));

vi.mock('@app/analytics', () => ({
    __esModule: true,
    default: { event: vi.fn() },
    EventType: { SetDeprecation: 'SetDeprecation' },
}));

const URN = 'urn:li:dataset:(urn:li:dataPlatform:mysql,my_db.my_schema.events,PROD)';
const FIELD_PATH = 'col_a';

describe('useUndeprecateResource', () => {
    let refetch: ReturnType<typeof vi.fn>;

    beforeEach(() => {
        vi.clearAllMocks();
        refetch = vi.fn();
        batchUpdateDeprecationMock.mockResolvedValue({ errors: undefined });
    });

    it('clears an asset deprecation and reports it without sub-resources', async () => {
        const { result } = renderHook(() => useUndeprecateResource({ urn: URN, refetch }));

        await expect(result.current()).resolves.toBe(true);

        expect(batchUpdateDeprecationMock).toHaveBeenCalledWith({
            variables: {
                input: {
                    resources: [{ resourceUrn: URN, subResource: undefined, subResourceType: undefined }],
                    deprecated: false,
                },
            },
        });
        expect(toast.success).toHaveBeenCalled();
        expect(refetch).toHaveBeenCalled();
        expect(analytics.event).toHaveBeenCalledWith({
            type: EventType.SetDeprecation,
            entityUrns: [URN],
            deprecated: false,
            resources: undefined,
        });
    });

    it('clears a column deprecation and reports which column it was', async () => {
        const { result } = renderHook(() =>
            useUndeprecateResource({
                urn: URN,
                subResource: FIELD_PATH,
                subResourceType: SubResourceType.DatasetField,
                refetch,
            }),
        );

        await result.current();

        const resources = [
            { resourceUrn: URN, subResource: FIELD_PATH, subResourceType: SubResourceType.DatasetField },
        ];
        expect(batchUpdateDeprecationMock).toHaveBeenCalledWith({
            variables: { input: { resources, deprecated: false } },
        });
        expect(analytics.event).toHaveBeenCalledWith(expect.objectContaining({ resources }));
    });

    it('still reports success when the refresh that follows it rejects', async () => {
        refetch.mockRejectedValue(new Error('Refresh failed'));
        const { result } = renderHook(() => useUndeprecateResource({ urn: URN, refetch }));

        await expect(result.current()).resolves.toBe(true);

        expect(toast.success).toHaveBeenCalled();
        expect(toast.error).not.toHaveBeenCalled();
    });

    it('still reports success when the refresh throws outright', async () => {
        refetch.mockImplementation(() => {
            throw new Error('Refresh exploded');
        });
        const { result } = renderHook(() => useUndeprecateResource({ urn: URN, refetch }));

        await expect(result.current()).resolves.toBe(true);

        expect(toast.success).toHaveBeenCalled();
        expect(toast.error).not.toHaveBeenCalled();
    });

    it('reports a mutation that comes back with GraphQL errors and does not refetch', async () => {
        batchUpdateDeprecationMock.mockResolvedValue({ errors: [{ message: 'Unauthorized' }] });
        const { result } = renderHook(() => useUndeprecateResource({ urn: URN, refetch }));

        await expect(result.current()).resolves.toBe(false);

        expect(toast.error).toHaveBeenCalled();
        expect(toast.success).not.toHaveBeenCalled();
        expect(refetch).not.toHaveBeenCalled();
        expect(analytics.event).not.toHaveBeenCalled();
    });

    it('surfaces a rejected mutation as an error toast and does not refetch', async () => {
        batchUpdateDeprecationMock.mockRejectedValue(new Error('Network down'));
        const { result } = renderHook(() => useUndeprecateResource({ urn: URN, refetch }));

        await expect(result.current()).resolves.toBe(false);

        expect(toast.error).toHaveBeenCalled();
        expect(toast.success).not.toHaveBeenCalled();
        expect(refetch).not.toHaveBeenCalled();
    });
});
