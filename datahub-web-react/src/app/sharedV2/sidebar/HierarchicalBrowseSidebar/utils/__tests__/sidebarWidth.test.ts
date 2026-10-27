import {
    SIDEBAR_MAX_WIDTH,
    SIDEBAR_MIN_WIDTH,
    SIDEBAR_WIDTH_STORAGE_KEY,
} from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/constants';
import {
    clampSidebarWidth,
    readStoredSidebarWidth,
    writeStoredSidebarWidth,
} from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/utils/sidebarWidth';

describe('clampSidebarWidth', () => {
    it('clamps below min and above max', () => {
        expect(clampSidebarWidth(100)).toBe(SIDEBAR_MIN_WIDTH);
        expect(clampSidebarWidth(900)).toBe(SIDEBAR_MAX_WIDTH);
    });

    it('passes through values in range', () => {
        expect(clampSidebarWidth(320)).toBe(320);
        expect(clampSidebarWidth(SIDEBAR_MIN_WIDTH)).toBe(SIDEBAR_MIN_WIDTH);
        expect(clampSidebarWidth(SIDEBAR_MAX_WIDTH)).toBe(SIDEBAR_MAX_WIDTH);
    });
});

describe('sidebar width localStorage helpers', () => {
    beforeEach(() => {
        localStorage.clear();
    });

    it('returns null when nothing is stored', () => {
        expect(readStoredSidebarWidth()).toBeNull();
    });

    it('writes a clamped width and reads it back', () => {
        writeStoredSidebarWidth(340);
        expect(localStorage.getItem(SIDEBAR_WIDTH_STORAGE_KEY)).toBe('340');
        expect(readStoredSidebarWidth()).toBe(340);
    });

    it('clamps on write and read', () => {
        writeStoredSidebarWidth(50);
        expect(readStoredSidebarWidth()).toBe(SIDEBAR_MIN_WIDTH);

        localStorage.setItem(SIDEBAR_WIDTH_STORAGE_KEY, '9999');
        expect(readStoredSidebarWidth()).toBe(SIDEBAR_MAX_WIDTH);
    });

    it('returns null for non-numeric storage', () => {
        localStorage.setItem(SIDEBAR_WIDTH_STORAGE_KEY, 'nope');
        expect(readStoredSidebarWidth()).toBeNull();
    });
});
