import { useCallback } from 'react';

import { FileUploadFailureType, FileUploadSource } from '@components/components/Editor/types';

import analytics, { EventType } from '@app/analytics';

import { UploadDownloadScenario } from '@types';

interface Props {
    scenario: UploadDownloadScenario;
    assetUrn?: string;
    schemaField?: string;
}

export default function useFileUploadAnalyticsCallbacks({ scenario, assetUrn, schemaField }: Props) {
    const onFileUploadAttempt = useCallback(
        (fileType: string, fileSize: number, source: FileUploadSource) => {
            analytics.event({
                type: EventType.FileUploadAttemptEvent,
                fileType,
                fileSize,
                scenario,
                source,
                assetUrn,
                schemaFieldUrn: schemaField,
            });
        },
        [scenario, assetUrn, schemaField],
    );

    const onFileUploadFailed = useCallback(
        (
            fileType: string,
            fileSize: number,
            source: FileUploadSource,
            failureType: FileUploadFailureType,
            comment?: string,
        ) => {
            analytics.event({
                type: EventType.FileUploadFailedEvent,
                fileType,
                fileSize,
                scenario,
                source,
                assetUrn,
                schemaFieldUrn: schemaField,
                failureType,
                comment,
            });
        },
        [scenario, assetUrn, schemaField],
    );

    const onFileUploadSucceeded = useCallback(
        (fileType: string, fileSize: number, source: FileUploadSource) => {
            analytics.event({
                type: EventType.FileUploadSucceededEvent,
                fileType,
                fileSize,
                scenario,
                source,
                assetUrn,
                schemaFieldUrn: schemaField,
            });
        },
        [scenario, assetUrn, schemaField],
    );

    const onFileDownloadView = useCallback(() => {
        analytics.event({
            type: EventType.FileDownloadViewEvent,
            // These fields aren't accessible while downloading
            // fileType,
            // fileSize,
            scenario,
            assetUrn,
            schemaFieldUrn: schemaField,
        });
    }, [scenario, assetUrn, schemaField]);

    return { onFileUploadAttempt, onFileUploadFailed, onFileUploadSucceeded, onFileDownloadView };
}
