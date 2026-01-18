import { describe, expect, it, vi } from 'vitest';

import { updateUrlParam } from '@app/shared/updateUrlParam';

// Mock the window.location
const mockWindowLocation = {
    href: 'http://localhost:3000/test?param1=value1',
};

// Mock window object
Object.defineProperty(window, 'location', {
    value: mockWindowLocation,
    writable: true,
});

// Mock URL and URLSearchParams
const originalURL = global.URL;
const originalURLSearchParams = global.URLSearchParams;

describe('updateUrlParam', () => {
    const mockHistory = {
        replace: vi.fn(),
    };

    beforeEach(() => {
        vi.clearAllMocks();
    });

    afterAll(() => {
        // Restore original URL and URLSearchParams
        global.URL = originalURL;
        global.URLSearchParams = originalURLSearchParams;
    });

    it('should replace history with new URL containing the parameter', () => {
        // Mock URL constructor
        const mockURLConstructor = vi.fn(function (this: any, url: string) {
            this.href = url;
            this.search = '?param1=value1&newParam=test';
            this.searchParams = {
                set: vi.fn(),
            };
        });
        global.URL = mockURLConstructor as any;

        updateUrlParam(mockHistory as any, 'newParam', 'test');

        expect(mockURLConstructor).toHaveBeenCalledWith('http://localhost:3000/test?param1=value1');
        expect(mockHistory.replace).toHaveBeenCalledWith('?param1=value1&newParam=test', undefined);
    });

    it('should replace history with new URL containing the parameter and state', () => {
        // Mock URL constructor
        const mockURLConstructor = vi.fn(function (this: any, url: string) {
            this.href = url;
            this.search = '?param1=value1&newParam=test';
            this.searchParams = {
                set: vi.fn(),
            };
        });
        global.URL = mockURLConstructor as any;

        const mockState = { some: 'state' };
        updateUrlParam(mockHistory as any, 'newParam', 'test', mockState);

        expect(mockURLConstructor).toHaveBeenCalledWith('http://localhost:3000/test?param1=value1');
        expect(mockHistory.replace).toHaveBeenCalledWith('?param1=value1&newParam=test', mockState);
    });

    it('should update existing parameter value', () => {
        // Mock URL constructor
        const mockURLConstructor = vi.fn(function (this: any, url: string) {
            this.href = url;
            this.search = '?param1=updated&existingParam=newValue';
            this.searchParams = {
                set: vi.fn(),
            };
        });
        global.URL = mockURLConstructor as any;

        updateUrlParam(mockHistory as any, 'existingParam', 'newValue');

        expect(mockURLConstructor).toHaveBeenCalledWith('http://localhost:3000/test?param1=value1');
        expect(mockHistory.replace).toHaveBeenCalledWith('?param1=updated&existingParam=newValue', undefined);
    });
});
