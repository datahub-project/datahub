import { describe, expect, it, vi } from 'vitest';

import { updateUrlParam } from '@app/sharedV2/updateUrlParam';

describe('updateUrlParam', () => {
    const mockHistory = {
        replace: vi.fn(),
        location: { search: '' },
    };

    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should replace history with new URL containing the parameter', () => {
        mockHistory.location.search = '?param1=value1';

        updateUrlParam(mockHistory as any, 'newParam', 'test');

        expect(mockHistory.replace).toHaveBeenCalledWith('?newParam=test&param1=value1', undefined);
    });

    it('should replace history with new URL containing the parameter and state', () => {
        mockHistory.location.search = '?param1=value1';

        const mockState = { some: 'state' };
        updateUrlParam(mockHistory as any, 'newParam', 'test', mockState);

        expect(mockHistory.replace).toHaveBeenCalledWith('?newParam=test&param1=value1', mockState);
    });

    it('should update existing parameter value', () => {
        mockHistory.location.search = '?param1=value1&existingParam=oldValue';

        updateUrlParam(mockHistory as any, 'existingParam', 'newValue');

        expect(mockHistory.replace).toHaveBeenCalledWith('?existingParam=newValue&param1=value1', undefined);
    });

    it('should preserve 3%2B encoding in existing params when adding a new param (regression: degree filter bug)', () => {
        // Simulates the Impact Analysis URL after user selects 3+ dependency levels.
        // Previously, the native URL.searchParams API would decode 3%2B → 3+ and re-encode as 3%20,
        // causing GMS to reject with "3  is not a valid filter value for degree filters".
        mockHistory.location.search = '?filter_degree___false___EQUAL___0=3%2B%2C2%2C1';

        updateUrlParam(mockHistory as any, 'sortOption', 'relevance');

        const call = mockHistory.replace.mock.calls[0][0] as string;
        expect(call).toContain('3%2B');
        expect(call).not.toContain('3%20');
        expect(call).not.toContain('3 ');
        expect(call).toContain('sortOption=relevance');
    });

    it('should encode special characters in the new value being set', () => {
        mockHistory.location.search = '';

        updateUrlParam(mockHistory as any, 'query', 'hello world');

        const call = mockHistory.replace.mock.calls[0][0] as string;
        expect(call).toContain('query=hello%20world');
        expect(call).not.toContain('hello world');
    });

    it('should remove the parameter when the value is null, keeping comma-separated values intact', () => {
        mockHistory.location.search = '?filter_platform___false___EQUAL___0=a%2Cb&showSiblings=true';

        updateUrlParam(mockHistory as any, 'showSiblings', null);

        expect(mockHistory.replace).toHaveBeenCalledWith('?filter_platform___false___EQUAL___0=a%2Cb', undefined);
    });
});
