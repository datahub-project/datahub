import { beforeEach, describe, expect, it, vi } from 'vitest';

/**
 * Tests for conversationStorage utilities
 */

describe('getOrCreateSessionId', () => {
    const mockSessionStorage = {
        store: {} as Record<string, string>,
        getItem: vi.fn((key: string) => mockSessionStorage.store[key] || null),
        setItem: vi.fn((key: string, value: string) => {
            mockSessionStorage.store[key] = value;
        }),
        removeItem: vi.fn((key: string) => {
            delete mockSessionStorage.store[key];
        }),
        clear: vi.fn(() => {
            mockSessionStorage.store = {};
        }),
        get length() {
            return Object.keys(mockSessionStorage.store).length;
        },
        key: vi.fn((index: number) => Object.keys(mockSessionStorage.store)[index] || null),
    };

    beforeEach(() => {
        // Reset mock
        mockSessionStorage.store = {};
        mockSessionStorage.getItem.mockClear();
        mockSessionStorage.setItem.mockClear();

        // Mock sessionStorage
        Object.defineProperty(window, 'sessionStorage', {
            value: mockSessionStorage,
            writable: true,
        });

        // Reset the module to clear the sessionIdCache
        vi.resetModules();
    });

    it('should generate a new session ID when none exists', async () => {
        const { getOrCreateSessionId } = await import('../conversationStorage');
        const sessionId = getOrCreateSessionId();

        expect(sessionId).toMatch(/^session_\d+_[\d.]+$/);
        expect(mockSessionStorage.setItem).toHaveBeenCalledWith('askDataHub:sessionId', sessionId);
    });

    it('should return existing session ID from sessionStorage', async () => {
        const existingId = 'session_123_0.456';
        mockSessionStorage.store['askDataHub:sessionId'] = existingId;

        const { getOrCreateSessionId } = await import('../conversationStorage');
        const sessionId = getOrCreateSessionId();

        expect(sessionId).toBe(existingId);
        expect(mockSessionStorage.setItem).not.toHaveBeenCalled();
    });

    it('should cache the session ID and return it on subsequent calls', async () => {
        const { getOrCreateSessionId } = await import('../conversationStorage');

        const firstCall = getOrCreateSessionId();
        const secondCall = getOrCreateSessionId();

        expect(firstCall).toBe(secondCall);
        // Only one getItem call - second call uses cache
        expect(mockSessionStorage.getItem).toHaveBeenCalledTimes(1);
    });

    it('should update cache when retrieving from sessionStorage', async () => {
        const existingId = 'session_existing_0.789';
        mockSessionStorage.store['askDataHub:sessionId'] = existingId;

        const { getOrCreateSessionId } = await import('../conversationStorage');

        const firstCall = getOrCreateSessionId();
        const secondCall = getOrCreateSessionId();

        expect(firstCall).toBe(existingId);
        expect(secondCall).toBe(existingId);
        // Only one getItem call - cache populated on first retrieval
        expect(mockSessionStorage.getItem).toHaveBeenCalledTimes(1);
    });

    it('should generate fallback ID when sessionStorage throws', async () => {
        mockSessionStorage.getItem.mockImplementation(() => {
            throw new Error('Storage unavailable');
        });

        const { getOrCreateSessionId } = await import('../conversationStorage');
        const sessionId = getOrCreateSessionId();

        expect(sessionId).toMatch(/^session_\d+_[\d.]+$/);
    });

    it('should cache fallback ID when sessionStorage is unavailable', async () => {
        mockSessionStorage.getItem.mockImplementation(() => {
            throw new Error('Storage unavailable');
        });

        const { getOrCreateSessionId } = await import('../conversationStorage');

        const firstCall = getOrCreateSessionId();
        const secondCall = getOrCreateSessionId();

        expect(firstCall).toBe(secondCall);
        // Second call uses cache, doesn't try storage again
        expect(mockSessionStorage.getItem).toHaveBeenCalledTimes(1);
    });
});
