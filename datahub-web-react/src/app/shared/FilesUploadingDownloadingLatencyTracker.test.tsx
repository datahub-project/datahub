import { render, screen } from '@testing-library/react';
import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import analytics, { EventType } from '@app/analytics';
import { FilesUploadingDownloadingLatencyTracker } from '@app/shared/FilesUploadingDownloadingLatencyTracker';

// Mock the analytics module before the import
vi.mock('@app/analytics', () => {
    const mockAnalyticsEvent = vi.fn();
    return {
        __esModule: true,
        default: {
            event: mockAnalyticsEvent,
        },
        EventType: {
            FileUploadLatencyEvent: 'FileUploadLatencyEvent',
            FileDownloadLatencyEvent: 'FileDownloadLatencyEvent',
        } as any,
    };
});

// Now import after the mock

// Mock the PerformanceObserver API
const mockObserve = vi.fn();
const mockDisconnect = vi.fn();

class MockPerformanceObserver {
    constructor(private callback: PerformanceObserverCallback) {}

    observe = mockObserve;

    disconnect = mockDisconnect;

    // Method to simulate the callback being called with entries (for testing)
    triggerCallback = (list: any) => {
        this.callback(list, this as any);
    };
}

// Replace the global PerformanceObserver with our mock
Object.defineProperty(window, 'PerformanceObserver', {
    writable: true,
    value: vi.fn((callback) => new MockPerformanceObserver(callback)),
});

// Mock performance.getEntriesByType to return empty arrays by default
Object.defineProperty(window, 'performance', {
    value: {
        getEntriesByType: vi.fn(() => []),
        getEntries: vi.fn(() => []),
    },
    writable: true,
});

describe('FilesUploadingDownloadingLatencyTracker', () => {
    beforeEach(() => {
        // Reset all mocks
        vi.clearAllMocks();
        vi.resetAllMocks();
    });

    afterEach(() => {
        vi.clearAllMocks();
    });

    it('renders without crashing and sets up PerformanceObserver', () => {
        render(<FilesUploadingDownloadingLatencyTracker />);

        // Component should render as null
        expect(screen.queryByRole('main')).toBeNull();

        // Should have created a PerformanceObserver instance
        expect(window.PerformanceObserver).toHaveBeenCalled();

        // Should have called observe with the right parameters
        expect(mockObserve).toHaveBeenCalledWith({ type: 'resource', buffered: true });
    });

    it('observes resource timing entries when performance entries are available', () => {
        const mockUploadEntry: Partial<PerformanceResourceTiming> = {
            entryType: 'resource',
            name: 'https://example.amazonaws.com/upload',
            initiatorType: 'fetch',
            duration: 100,
        };

        const mockDownloadEntry: Partial<PerformanceResourceTiming> = {
            entryType: 'resource',
            name: 'https://api.example.com/openapi/v1/files/download',
            initiatorType: 'xmlhttprequest',
            duration: 200,
        };

        render(<FilesUploadingDownloadingLatencyTracker />);

        // Get the callback that was passed to PerformanceObserver constructor
        const callback = (window.PerformanceObserver as any).mock.calls[0][0];

        // Simulate the callback with our entries
        callback(
            {
                getEntries: () => [mockUploadEntry, mockDownloadEntry],
            },
            new MockPerformanceObserver(() => {}),
        );

        // Check that analytics events were triggered
        expect(analytics.event).toHaveBeenCalledTimes(2);
        expect(analytics.event).toHaveBeenCalledWith({
            type: EventType.FileUploadLatencyEvent,
            url: mockUploadEntry.name,
            duration: mockUploadEntry.duration,
        });
        expect(analytics.event).toHaveBeenCalledWith({
            type: EventType.FileDownloadLatencyEvent,
            url: mockDownloadEntry.name,
            duration: mockDownloadEntry.duration,
        });
    });

    it('filters out non-resource entries', () => {
        const mockNavigationEntry: Partial<PerformanceEntry> = {
            entryType: 'navigation',
            name: 'https://example.com',
            duration: 150,
        };

        render(<FilesUploadingDownloadingLatencyTracker />);

        // Get the callback that was passed to PerformanceObserver constructor
        const callback = (window.PerformanceObserver as any).mock.calls[0][0];

        // Simulate the callback with a non-resource entry
        callback(
            {
                getEntries: () => [mockNavigationEntry],
            },
            new MockPerformanceObserver(() => {}),
        );

        // Analytics should not be called for non-resource entries
        expect(analytics.event).not.toHaveBeenCalled();
    });

    it('identifies upload entries correctly', () => {
        const mockUploadEntry: Partial<PerformanceResourceTiming> = {
            entryType: 'resource',
            name: 'https://bucket.s3.amazonaws.com/files',
            initiatorType: 'fetch',
            duration: 50,
        };

        const mockNonUploadEntry: Partial<PerformanceResourceTiming> = {
            entryType: 'resource',
            name: 'https://googleapis.com/some-api',
            initiatorType: 'fetch',
            duration: 50,
        };

        render(<FilesUploadingDownloadingLatencyTracker />);

        // Get the callback that was passed to PerformanceObserver constructor
        const callback = (window.PerformanceObserver as any).mock.calls[0][0];

        // Simulate the callback with both entries
        callback(
            {
                getEntries: () => [mockUploadEntry, mockNonUploadEntry],
            },
            new MockPerformanceObserver(() => {}),
        );

        // Only the upload entry should trigger an event
        expect(analytics.event).toHaveBeenCalledTimes(1);
        expect(analytics.event).toHaveBeenCalledWith({
            type: EventType.FileUploadLatencyEvent,
            url: mockUploadEntry.name,
            duration: mockUploadEntry.duration,
        });
    });

    it('identifies download entries correctly', () => {
        const mockDownloadEntry: Partial<PerformanceResourceTiming> = {
            entryType: 'resource',
            name: 'https://api.example.com/openapi/v1/files/download',
            initiatorType: 'xmlhttprequest',
            duration: 75,
        };

        const mockNonDownloadEntry: Partial<PerformanceResourceTiming> = {
            entryType: 'resource',
            name: 'https://cdn.example.com/other-files',
            initiatorType: 'xmlhttprequest',
            duration: 75,
        };

        render(<FilesUploadingDownloadingLatencyTracker />);

        // Get the callback that was passed to PerformanceObserver constructor
        const callback = (window.PerformanceObserver as any).mock.calls[0][0];

        // Simulate the callback with both entries
        callback(
            {
                getEntries: () => [mockDownloadEntry, mockNonDownloadEntry],
            },
            new MockPerformanceObserver(() => {}),
        );

        // Only the download entry should trigger an event
        expect(analytics.event).toHaveBeenCalledTimes(1);
        expect(analytics.event).toHaveBeenCalledWith({
            type: EventType.FileDownloadLatencyEvent,
            url: mockDownloadEntry.name,
            duration: mockDownloadEntry.duration,
        });
    });

    it('disconnects PerformanceObserver on unmount', () => {
        const { unmount } = render(<FilesUploadingDownloadingLatencyTracker />);

        // Initially, disconnect should not have been called
        expect(mockDisconnect).not.toHaveBeenCalled();

        unmount();

        expect(mockDisconnect).toHaveBeenCalled();
    });

    it('does not trigger analytics for entries that are neither uploads nor downloads', () => {
        const mockOtherEntry: Partial<PerformanceResourceTiming> = {
            entryType: 'resource',
            name: 'https://some-other-domain.com/other-resource',
            initiatorType: 'img',
            duration: 100,
        };

        render(<FilesUploadingDownloadingLatencyTracker />);

        // Get the callback that was passed to PerformanceObserver constructor
        const callback = (window.PerformanceObserver as any).mock.calls[0][0];

        // Simulate the callback with the other entry
        callback(
            {
                getEntries: () => [mockOtherEntry],
            },
            new MockPerformanceObserver(() => {}),
        );

        // No analytics events should be triggered for non-upload/download entries
        expect(analytics.event).not.toHaveBeenCalled();
    });
});
