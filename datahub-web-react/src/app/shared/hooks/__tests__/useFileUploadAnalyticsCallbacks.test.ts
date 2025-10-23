import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { FileUploadFailureType, FileUploadSource } from '@components/components/Editor/types';

import analytics from '@app/analytics';
import { EventType } from '@app/analytics/event';
import useFileUploadAnalyticsCallbacks from '@app/shared/hooks/useFileUploadAnalyticsCallbacks';

import { UploadDownloadScenario } from '@types';

// Mock analytics module
vi.mock('@app/analytics', async () => {
    const actual = await vi.importActual('@app/analytics');
    return {
        default: {
            event: vi.fn(),
        },
        EventType: actual.EventType,
    };
});

describe('useFileUploadAnalyticsCallbacks', () => {
    const mockScenario = UploadDownloadScenario.AssetDocumentation;
    const mockAssetUrn = 'urn:li:dataset:(urn:li:dataPlatform:mysql,abc,PROD)';
    const mockSchemaField = 'urn:li:schemaField:abc';

    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('onFileUploadAttempt', () => {
        it('should call analytics event with correct properties for upload attempt', () => {
            const { result } = renderHook(() =>
                useFileUploadAnalyticsCallbacks({
                    scenario: mockScenario,
                    assetUrn: mockAssetUrn,
                    schemaField: mockSchemaField,
                }),
            );

            const fileType = 'application/pdf';
            const fileSize = 1024;
            const source: FileUploadSource = 'drag-and-drop';

            result.current.onFileUploadAttempt(fileType, fileSize, source);

            expect(analytics.event).toHaveBeenCalledWith({
                type: EventType.FileUploadAttemptEvent,
                fileType,
                fileSize,
                scenario: mockScenario,
                source,
                assetUrn: mockAssetUrn,
                schemaFieldUrn: mockSchemaField,
            });
        });

        it('should call analytics event with button source', () => {
            const { result } = renderHook(() =>
                useFileUploadAnalyticsCallbacks({
                    scenario: mockScenario,
                    assetUrn: mockAssetUrn,
                }),
            );

            const fileType = 'image/png';
            const fileSize = 2048;
            const source: FileUploadSource = 'button';

            result.current.onFileUploadAttempt(fileType, fileSize, source);

            expect(analytics.event).toHaveBeenCalledWith({
                type: EventType.FileUploadAttemptEvent,
                fileType,
                fileSize,
                scenario: mockScenario,
                source,
                assetUrn: mockAssetUrn,
                schemaFieldUrn: undefined,
            });
        });
    });

    describe('onFileUploadFailed', () => {
        it('should call analytics event with correct properties for upload failure', () => {
            const { result } = renderHook(() =>
                useFileUploadAnalyticsCallbacks({
                    scenario: mockScenario,
                    assetUrn: mockAssetUrn,
                    schemaField: mockSchemaField,
                }),
            );

            const fileType = 'application/pdf';
            const fileSize = 1024;
            const source: FileUploadSource = 'drag-and-drop';
            const failureType = FileUploadFailureType.FILE_SIZE;
            const comment = 'File too large';

            result.current.onFileUploadFailed(fileType, fileSize, source, failureType, comment);

            expect(analytics.event).toHaveBeenCalledWith({
                type: EventType.FileUploadFailedEvent,
                fileType,
                fileSize,
                scenario: mockScenario,
                source,
                assetUrn: mockAssetUrn,
                schemaFieldUrn: mockSchemaField,
                failureType,
                comment,
            });
        });

        it('should call analytics event without comment when not provided', () => {
            const { result } = renderHook(() =>
                useFileUploadAnalyticsCallbacks({
                    scenario: mockScenario,
                    assetUrn: mockAssetUrn,
                }),
            );

            const fileType = 'application/pdf';
            const fileSize = 1024;
            const source: FileUploadSource = 'button';
            const failureType = FileUploadFailureType.FILE_TYPE;

            result.current.onFileUploadFailed(fileType, fileSize, source, failureType);

            expect(analytics.event).toHaveBeenCalledWith({
                type: EventType.FileUploadFailedEvent,
                fileType,
                fileSize,
                scenario: mockScenario,
                source,
                assetUrn: mockAssetUrn,
                schemaFieldUrn: undefined,
                failureType,
                comment: undefined,
            });
        });
    });

    describe('onFileUploadSucceeded', () => {
        it('should call analytics event with correct properties for upload success', () => {
            const { result } = renderHook(() =>
                useFileUploadAnalyticsCallbacks({
                    scenario: mockScenario,
                    assetUrn: mockAssetUrn,
                    schemaField: mockSchemaField,
                }),
            );

            const fileType = 'application/pdf';
            const fileSize = 1024;
            const source: FileUploadSource = 'drag-and-drop';

            result.current.onFileUploadSucceeded(fileType, fileSize, source);

            expect(analytics.event).toHaveBeenCalledWith({
                type: EventType.FileUploadSucceededEvent,
                fileType,
                fileSize,
                scenario: mockScenario,
                source,
                assetUrn: mockAssetUrn,
                schemaFieldUrn: mockSchemaField,
            });
        });

        it('should call analytics event from button source', () => {
            const { result } = renderHook(() =>
                useFileUploadAnalyticsCallbacks({
                    scenario: mockScenario,
                }),
            );

            const fileType = 'image/png';
            const fileSize = 2048;
            const source: FileUploadSource = 'button';

            result.current.onFileUploadSucceeded(fileType, fileSize, source);

            expect(analytics.event).toHaveBeenCalledWith({
                type: EventType.FileUploadSucceededEvent,
                fileType,
                fileSize,
                scenario: mockScenario,
                source,
                assetUrn: undefined,
                schemaFieldUrn: undefined,
            });
        });
    });

    describe('onFileDownloadView', () => {
        it('should call analytics event with correct properties for file download/view', () => {
            const { result } = renderHook(() =>
                useFileUploadAnalyticsCallbacks({
                    scenario: mockScenario,
                    assetUrn: mockAssetUrn,
                    schemaField: mockSchemaField,
                }),
            );

            result.current.onFileDownloadView();

            expect(analytics.event).toHaveBeenCalledWith({
                type: EventType.FileDownloadViewEvent,
                scenario: mockScenario,
                assetUrn: mockAssetUrn,
                schemaFieldUrn: mockSchemaField,
            });
        });

        it('should call analytics event with minimal properties', () => {
            const { result } = renderHook(() =>
                useFileUploadAnalyticsCallbacks({
                    scenario: mockScenario,
                }),
            );

            result.current.onFileDownloadView();

            expect(analytics.event).toHaveBeenCalledWith({
                type: EventType.FileDownloadViewEvent,
                scenario: mockScenario,
                assetUrn: undefined,
                schemaFieldUrn: undefined,
            });
        });
    });

    describe('memoization', () => {
        it('should memoize the callback functions', () => {
            const { result, rerender } = renderHook((props) => useFileUploadAnalyticsCallbacks(props), {
                initialProps: {
                    scenario: mockScenario,
                    assetUrn: mockAssetUrn,
                },
            });

            const initialCallbacks = result.current;

            // Rerender with same props
            rerender({
                scenario: mockScenario,
                assetUrn: mockAssetUrn,
            });

            // Functions should be the same reference (memoized)
            expect(result.current.onFileUploadAttempt).toBe(initialCallbacks.onFileUploadAttempt);
            expect(result.current.onFileUploadFailed).toBe(initialCallbacks.onFileUploadFailed);
            expect(result.current.onFileUploadSucceeded).toBe(initialCallbacks.onFileUploadSucceeded);
            expect(result.current.onFileDownloadView).toBe(initialCallbacks.onFileDownloadView);
        });

        it('should update callbacks when dependencies change', () => {
            const { result, rerender } = renderHook((props) => useFileUploadAnalyticsCallbacks(props), {
                initialProps: {
                    scenario: mockScenario,
                    assetUrn: mockAssetUrn,
                },
            });

            const initialCallbacks = result.current;
            const newAssetUrn = 'urn:li:dataset:(urn:li:dataPlatform:mysql,xyz,PROD)';

            // Rerender with different props
            rerender({
                scenario: mockScenario,
                assetUrn: newAssetUrn,
            });

            // Functions should be different references since dependencies changed
            expect(result.current.onFileUploadAttempt).not.toBe(initialCallbacks.onFileUploadAttempt);
            expect(result.current.onFileUploadFailed).not.toBe(initialCallbacks.onFileUploadFailed);
            expect(result.current.onFileUploadSucceeded).not.toBe(initialCallbacks.onFileUploadSucceeded);
            expect(result.current.onFileDownloadView).not.toBe(initialCallbacks.onFileDownloadView);
        });
    });
});
