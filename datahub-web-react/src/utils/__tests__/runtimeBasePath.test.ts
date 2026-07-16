import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import {
    getBasePath,
    getRuntimeBasePath,
    removeRuntimePath,
    resetCachedBasePath,
    resolveRuntimePath,
} from '@utils/runtimeBasePath';

// Mock DOM methods
const mockQuerySelector = vi.fn();
const mockGetAttribute = vi.fn();

// Mock document
Object.defineProperty(global, 'document', {
    value: {
        querySelector: mockQuerySelector,
    },
    writable: true,
});

// Mock window
Object.defineProperty(global, 'window', {
    value: {},
    writable: true,
});

// Store original env
const originalEnv = process.env;

describe('runtimeBasePath', () => {
    beforeEach(() => {
        // Reset mocks
        vi.clearAllMocks();
        mockQuerySelector.mockReturnValue(null);
        mockGetAttribute.mockReturnValue(null);

        // Reset window
        (global.window as any) = {};

        // Reset process.env
        process.env = { ...originalEnv };

        // Reset the module's internal cache by re-importing
        vi.resetModules();
        resetCachedBasePath();
    });

    afterEach(() => {
        process.env = originalEnv;
    });

    describe('getBasePath', () => {
        it('should return "/" in development environment', () => {
            process.env.NODE_ENV = 'development';

            const result = getBasePath();

            expect(result).toBe('/');
        });

        it('should return empty string when base tag href is "/"', () => {
            process.env.NODE_ENV = 'production';
            const mockBaseElement = { getAttribute: mockGetAttribute };
            mockQuerySelector.mockReturnValue(mockBaseElement);
            mockGetAttribute.mockReturnValue('/');

            const result = getBasePath();

            expect(result).toBe('');
        });

        it('should return base path without trailing slash', () => {
            process.env.NODE_ENV = 'production';
            const mockBaseElement = { getAttribute: mockGetAttribute };
            mockQuerySelector.mockReturnValue(mockBaseElement);
            mockGetAttribute.mockReturnValue('/datahub/');

            const result = getBasePath();

            expect(result).toBe('/datahub');
        });

        it('should use global variable when base tag is not available', () => {
            process.env.NODE_ENV = 'production';
            mockQuerySelector.mockReturnValue(null);
            (global.window as any).__DATAHUB_BASE_PATH__ = '/datahub/';

            const result = getBasePath();

            expect(result).toBe('/datahub');
        });

        it('should return empty string when no base path is found', () => {
            process.env.NODE_ENV = 'production';
            mockQuerySelector.mockReturnValue(null);

            const result = getBasePath();

            expect(result).toBe('');
        });
    });

    describe('getRuntimeBasePath', () => {
        it('should cache the result', () => {
            process.env.NODE_ENV = 'development';

            const result1 = getRuntimeBasePath();
            const result2 = getRuntimeBasePath();

            expect(result1).toBe('/');
            expect(result2).toBe('/');
        });
    });

    describe('resolveRuntimePath', () => {
        it('should return path as-is when base path is "/"', () => {
            process.env.NODE_ENV = 'development';

            const result = resolveRuntimePath('/api/graphql');

            expect(result).toBe('/api/graphql');
        });

        it('should prepend base path to absolute paths', () => {
            process.env.NODE_ENV = 'production';
            const mockBaseElement = { getAttribute: mockGetAttribute };
            mockQuerySelector.mockReturnValue(mockBaseElement);
            mockGetAttribute.mockReturnValue('/datahub');

            const result = resolveRuntimePath('/api/graphql');

            expect(result).toBe('/datahub/api/graphql');
        });

        it('should prepend base path to relative paths', () => {
            process.env.NODE_ENV = 'production';
            const mockBaseElement = { getAttribute: mockGetAttribute };
            mockQuerySelector.mockReturnValue(mockBaseElement);
            mockGetAttribute.mockReturnValue('/datahub');

            const result = resolveRuntimePath('api/graphql');

            expect(result).toBe('/datahub/api/graphql');
        });

        it('should return path as-is if it already starts with base path', () => {
            process.env.NODE_ENV = 'production';
            const mockBaseElement = { getAttribute: mockGetAttribute };
            mockQuerySelector.mockReturnValue(mockBaseElement);
            mockGetAttribute.mockReturnValue('/datahub');

            const result = resolveRuntimePath('/datahub/api/graphql');

            expect(result).toBe('/datahub/api/graphql');
        });
    });

    describe('removeRuntimePath', () => {
        it('should return path as-is when base path is "/"', () => {
            process.env.NODE_ENV = 'development';

            const result = removeRuntimePath('/api/graphql');

            expect(result).toBe('/api/graphql');
        });

        it('should remove base path from paths that start with it', () => {
            process.env.NODE_ENV = 'production';
            const mockBaseElement = { getAttribute: mockGetAttribute };
            mockQuerySelector.mockReturnValue(mockBaseElement);
            mockGetAttribute.mockReturnValue('/datahub');

            const result = removeRuntimePath('/datahub/api/graphql');

            expect(result).toBe('/api/graphql');
        });

        it('should return path as-is if it does not start with base path', () => {
            process.env.NODE_ENV = 'production';
            const mockBaseElement = { getAttribute: mockGetAttribute };
            mockQuerySelector.mockReturnValue(mockBaseElement);
            mockGetAttribute.mockReturnValue('/datahub');

            const result = removeRuntimePath('/api/graphql');

            expect(result).toBe('/api/graphql');
        });
    });

    describe('integration', () => {
        it('should work together: resolve then remove should return original path', () => {
            process.env.NODE_ENV = 'production';
            const mockBaseElement = { getAttribute: mockGetAttribute };
            mockQuerySelector.mockReturnValue(mockBaseElement);
            mockGetAttribute.mockReturnValue('/datahub');

            const originalPath = '/api/graphql';
            const resolvedPath = resolveRuntimePath(originalPath);
            const removedPath = removeRuntimePath(resolvedPath);

            expect(resolvedPath).toBe('/datahub/api/graphql');
            expect(removedPath).toBe('/api/graphql');
        });
    });
});
