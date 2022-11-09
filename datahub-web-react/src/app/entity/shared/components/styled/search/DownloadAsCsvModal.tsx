import React, { useState } from 'react';
import { Button, Input, Modal } from 'antd';
import { useLocation } from 'react-router';

import { EntityType, OrFilter, SearchAcrossEntitiesInput } from '../../../../../../types.generated';
import { SearchResultsInterface } from './types';
import { getSearchCsvDownloadHeader, transformResultsToCsvRow } from './downloadAsCsvUtil';
import { downloadRowsAsCsv } from '../../../../../search/utils/csvUtils';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { useEntityData } from '../../../EntityContext';
import analytics, { EventType } from '../../../../../analytics';

type Props = {
    callSearchOnVariables: (variables: {
        input: SearchAcrossEntitiesInput;
    }) => Promise<SearchResultsInterface | null | undefined>;
    entityFilters: EntityType[];
    filters: OrFilter[];
    query: string;
    setIsDownloadingCsv: (isDownloadingCsv: boolean) => any;
    showDownloadAsCsvModal: boolean;
    setShowDownloadAsCsvModal: (showDownloadAsCsvModal: boolean) => any;
};

const SEARCH_PAGE_SIZE_FOR_DOWNLOAD = 1000;

export default function DownloadAsCsvModal({
    callSearchOnVariables,
    entityFilters,
    filters,
    query,
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

    const triggerCsvDownload = (filename) => {
        setIsDownloadingCsv(true);
        console.log('preparing your csv');

        let downloadPage = 0;
        let accumulatedResults: string[][] = [];

        analytics.event({
            type: EventType.DownloadAsCsvEvent,
            query,
            entityUrn: entitySearchIsEmbeddedWithin?.urn,
            path: location.pathname,
        });

        function fetchNextPage() {
            console.log('fetch page number ', downloadPage);
            callSearchOnVariables({
                input: {
                    types: entityFilters,
                    query,
                    start: SEARCH_PAGE_SIZE_FOR_DOWNLOAD * downloadPage,
                    count: SEARCH_PAGE_SIZE_FOR_DOWNLOAD,
                    orFilters: filters,
                },
            }).then((refetchData) => {
                console.log('fetched data for page number ', downloadPage);
                accumulatedResults = [
                    ...accumulatedResults,
                    ...transformResultsToCsvRow(refetchData?.searchResults || [], entityRegistry),
                ];
                if ((refetchData?.start || 0) + (refetchData?.count || 0) < (refetchData?.total || 0)) {
                    downloadPage += 1;
                    fetchNextPage();
                } else {
                    setIsDownloadingCsv(false);
                    downloadRowsAsCsv(
                        getSearchCsvDownloadHeader(refetchData?.searchResults[0]),
                        accumulatedResults,
                        filename,
                    );
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
                placeholder="datahub.csv"
                value={saveAsTitle}
                onChange={(e) => {
                    setSaveAsTitle(e.target.value);
                }}
            />
        </Modal>
    );
}
