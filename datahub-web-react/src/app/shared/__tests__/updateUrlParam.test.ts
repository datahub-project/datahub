import { describe, expect, it, vi } from 'vitest';

import { updateUrlParam } from '@app/shared/updateUrlParam';

describe('updateUrlParam', () => {
    const mockHistory = {
        replace: vi.fn(),
    };

    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should replace history with new URL containing the parameter', () => {
        Object.defineProperty(window, 'location', {
            value: { search: '?param1=value1' },
            writable: true,
        });

        updateUrlParam(mockHistory as any, 'newParam', 'test');

        expect(mockHistory.replace).toHaveBeenCalledWith('?newParam=test&param1=value1', undefined);
    });

    it('should replace history with new URL containing the parameter and state', () => {
        Object.defineProperty(window, 'location', {
            value: { search: '?param1=value1' },
            writable: true,
        });

        const mockState = { some: 'state' };
        updateUrlParam(mockHistory as any, 'newParam', 'test', mockState);

        expect(mockHistory.replace).toHaveBeenCalledWith('?newParam=test&param1=value1', mockState);
    });

    it('should update existing parameter value', () => {
        Object.defineProperty(window, 'location', {
            value: { search: '?param1=value1&existingParam=oldValue' },
            writable: true,
        });

        updateUrlParam(mockHistory as any, 'existingParam', 'newValue');

        expect(mockHistory.replace).toHaveBeenCalledWith('?existingParam=newValue&param1=value1', undefined);
    });

    it('should preserve 3%2B encoding in existing params when adding a new param (regression: degree filter bug)', () => {
        // Simulates the Impact Analysis URL after user selects 3+ dependency levels.
        // Previously, the native URL.searchParams API would decode 3%2B → 3+ and re-encode as 3%20,
        // causing GMS to reject with "3  is not a valid filter value for degree filters".
        Object.defineProperty(window, 'location', {
            value: { search: '?filter_degree___false___EQUAL___0=3%2B%2C2%2C1' },
            writable: true,
        });

        updateUrlParam(mockHistory as any, 'sortOption', 'relevance');

        const call = mockHistory.replace.mock.calls[0][0] as string;
        expect(call).toContain('3%2B');
        expect(call).not.toContain('3%20');
        expect(call).not.toContain('3 ');
        expect(call).toContain('sortOption=relevance');
    });

    it('should encode special characters in the new value being set', () => {
        Object.defineProperty(window, 'location', {
            value: { search: '' },
            writable: true,
        });

        updateUrlParam(mockHistory as any, 'query', 'hello world');

        const call = mockHistory.replace.mock.calls[0][0] as string;
        expect(call).toContain('query=hello%20world');
        expect(call).not.toContain('hello world');
    });
});
