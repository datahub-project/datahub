import React, { useState } from 'react';
import { useLocation } from 'react-router';
import { Button, Input, Modal, Spin, notification } from 'antd';
import { LoadingOutlined } from '@ant-design/icons';
import { AndFilterInput } from '../../../../../../types.generated';
import { getSearchCsvDownloadHeader, transformResultsToCsvRow } from './downloadAsCsvUtil';
import { downloadRowsAsCsv } from '../../../../../search/utils/csvUtils';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { useEntityData } from '../../../../../entity/shared/EntityContext';
import analytics, { EventType } from '../../../../../analytics';
import { DownloadSearchResultsInput, DownloadSearchResults } from '../../../../../search/utils/types';

type Props = {
    downloadSearchResults: (input: DownloadSearchResultsInput) => Promise<DownloadSearchResults | null | undefined>;
    filters: AndFilterInput[];
    query: string;
    viewUrn?: string;
    totalResults?: number;
    setIsDownloadingCsv: (isDownloadingCsv: boolean) => any;
    showDownloadAsCsvModal: boolean;
    setShowDownloadAsCsvModal: (showDownloadAsCsvModal: boolean) => any;
};

const SEARCH_PAGE_SIZE_FOR_DOWNLOAD = 200;
const DOWNLOAD_NOTIFICATION_KEY = 'download-csv-notification';
const formatTime = (seconds: number) => {
    if (seconds < 60) return `${Math.round(seconds)}s`;
    const minutes = Math.floor(seconds / 60);
    const remainingSeconds = Math.round(seconds % 60);
    return `${minutes}m ${remainingSeconds}s`;
};

export default function DownloadAsCsvModal({
    downloadSearchResults,
    filters,
    query,
    viewUrn,
    totalResults,
    setIsDownloadingCsv,
    showDownloadAsCsvModal,
    setShowDownloadAsCsvModal,
}: Props) {
    const { entityData: entitySearchIsEmbeddedWithin } = useEntityData();
    const location = useLocation();

    const [saveAsTitle, setSaveAsTitle] = useState(
        entitySearchIsEmbeddedWithin ? `${entitySearchIsEmbeddedWithin.name}_impact.csv` : 'results.csv',
    );
    const entityRegistry = useEntityRegistry();
    const openNotification = (currentCount = 0, estimatedTimeRemaining?: number) => {
        let description =
            totalResults && currentCount < totalResults
                ? `Downloading ${currentCount} of ${totalResults} entities...`
                : 'Creating CSV to download';

        if (estimatedTimeRemaining !== undefined) {
            description += `\nEstimated time remaining: ${formatTime(estimatedTimeRemaining)}`;
        }

        notification.info({
            key: DOWNLOAD_NOTIFICATION_KEY,
            message: 'Preparing Download',
            description,
            placement: 'bottomRight',
            duration: null,
            icon: <Spin indicator={<LoadingOutlined style={{ fontSize: 24 }} spin />} />,
        });
    };

    const closeNotification = () => {
        setTimeout(() => {
            notification.destroy();
        }, 3000);
    };

    const showFailedDownloadNotification = () => {
        notification.destroy();
        notification.error({
            message: 'Download Failed',
            description: 'The CSV file could not be downloaded',
            placement: 'bottomRight',
            duration: 3,
        });
    };

    const triggerCsvDownload = (filename) => {
        setIsDownloadingCsv(true);
        openNotification();

        let nextScrollId: string | null = null;
        let accumulatedResults: string[][] = [];

        analytics.event({
            type: EventType.DownloadAsCsvEvent,
            query,
            entityUrn: entitySearchIsEmbeddedWithin?.urn,
            path: location.pathname,
        });

        let sizeForDownload = SEARCH_PAGE_SIZE_FOR_DOWNLOAD;
        let accumulatedTime = 0;
        let batchesProcessed = 0;

        function fetchNextPage() {
            const startTime = new Date().getTime();
            downloadSearchResults({
                scrollId: nextScrollId,
                query,
                count: sizeForDownload,
                orFilters: filters,
                viewUrn,
            })
                .then((refetchData) => {
                    const endTime = new Date().getTime();
                    const batchTime = endTime - startTime;
                    accumulatedTime += batchTime;
                    batchesProcessed++;

                    accumulatedResults = [
                        ...accumulatedResults,
                        ...transformResultsToCsvRow(refetchData?.searchResults || [], entityRegistry),
                    ];
                    // Scroll Across Entities gives max 10k as total but results can go further than that
                    if (totalResults && batchesProcessed > 0 && totalResults - accumulatedResults.length > 0) {
                        const averageTimePerBatch = accumulatedTime / batchesProcessed;
                        const remainingItems = totalResults - accumulatedResults.length;
                        const remainingBatches = Math.ceil(remainingItems / sizeForDownload);
                        const estimatedTimeRemaining = (averageTimePerBatch * remainingBatches) / 1000;
                        openNotification(accumulatedResults.length, estimatedTimeRemaining);
                    } else {
                        openNotification(accumulatedResults.length);
                    }
                    // If we have a "next offset", then we continue.
                    // Otherwise, we terminate fetching.
                    if (refetchData?.nextScrollId) {
                        nextScrollId = refetchData?.nextScrollId;
                        fetchNextPage();
                    } else {
                        setIsDownloadingCsv(false);
                        closeNotification();
                        downloadRowsAsCsv(
                            getSearchCsvDownloadHeader(refetchData?.searchResults[0]),
                            accumulatedResults,
                            filename,
                        );
                    }
                })
                .catch((_) => {
                    if (sizeForDownload > 10) {
                        sizeForDownload = Math.floor(sizeForDownload / 2);
                        console.log(`Failed to download, retrying with smaller page size of ${sizeForDownload}`);
                        fetchNextPage();
                    } else {
                        setIsDownloadingCsv(false);
                        showFailedDownloadNotification();
                    }
                });
        }
        fetchNextPage();
    };

    return (
        <Modal
            centered
            onCancel={() => setShowDownloadAsCsvModal(false)}
            title="Download as..."
            visible={showDownloadAsCsvModal}
            footer={
                <>
                    <Button onClick={() => setShowDownloadAsCsvModal(false)} type="text">
                        Close
                    </Button>
                    <Button
                        data-testid="csv-modal-download-button"
                        onClick={() => {
                            setShowDownloadAsCsvModal(false);
                            triggerCsvDownload(saveAsTitle);
                        }}
                        disabled={saveAsTitle.length === 0}
                    >
                        Download
                    </Button>
                </>
            }
        >
            <Input
                data-testid="download-as-csv-input"
                placeholder="datahub.csv"
                value={saveAsTitle}
                onChange={(e) => {
                    setSaveAsTitle(e.target.value);
                }}
            />
        </Modal>
    );
}
