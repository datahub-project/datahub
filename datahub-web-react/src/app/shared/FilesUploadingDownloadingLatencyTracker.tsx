import { useEffect } from 'react';

import analytics, { EventType } from '@app/analytics';

function isResource(entry: PerformanceEntry): entry is PerformanceResourceTiming {
    return entry.entryType === 'resource';
}

function isUploading(entry: PerformanceResourceTiming) {
    // S3 presigned upload URLs have X-Amz-Signature query parameter
    // This specifically identifies S3 uploads and excludes all other requests
    try {
        const url = new URL(entry.name);
        return (
            url.hostname.endsWith('.amazonaws.com') &&
            url.searchParams.has('X-Amz-Signature') &&
            entry.initiatorType === 'fetch'
        );
    } catch {
        return false;
    }
}

function isDownloading(entry: PerformanceResourceTiming) {
    return entry.name.includes('openapi/v1/files');
}

export function FilesUploadingDownloadingLatencyTracker() {
    useEffect(() => {
        const observer = new PerformanceObserver((list) => {
            list.getEntries()
                .filter(isResource)
                .forEach((entry) => {
                    if (isUploading(entry)) {
                        analytics.event({
                            type: EventType.FileUploadLatencyEvent,
                            url: entry.name,
                            duration: entry.duration,
                        });
                    } else if (isDownloading(entry)) {
                        analytics.event({
                            type: EventType.FileDownloadLatencyEvent,
                            url: entry.name,
                            duration: entry.duration,
                        });
                    }
                });
        });

        // Start observing resource timing entries
        observer.observe({ type: 'resource', buffered: true });

        return () => observer.disconnect();
    }, []);

    return null;
}
