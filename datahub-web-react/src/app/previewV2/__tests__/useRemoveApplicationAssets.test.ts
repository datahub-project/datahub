import { act } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import { useRemoveApplicationAssets } from '@app/previewV2/utils';
import { renderHook } from '@utils/test-utils/renderHook';

const mocks = vi.hoisted(() => ({
    batchSet: vi.fn(() => Promise.resolve({})),
    batchUnset: vi.fn(() => Promise.resolve({})),
}));

vi.mock('@graphql/application.generated', () => ({
    useBatchSetApplicationMutation: () => [mocks.batchSet],
    useBatchUnsetApplicationMutation: () => [mocks.batchUnset],
}));

describe('useRemoveApplicationAssets', () => {
    it('unsets only the given Application, keeping the asset in its other Applications', () => {
        const { result } = renderHook(() => useRemoveApplicationAssets(vi.fn()));

        act(() => result.current.removeApplication('urn:li:dataset:asset', 'urn:li:application:a'));
        act(() => result.current.confirmState.handleConfirm());

        expect(mocks.batchUnset).toHaveBeenCalledWith({
            variables: { input: { applicationUrn: 'urn:li:application:a', resourceUrns: ['urn:li:dataset:asset'] } },
        });
        expect(mocks.batchSet).not.toHaveBeenCalled();
    });
});
