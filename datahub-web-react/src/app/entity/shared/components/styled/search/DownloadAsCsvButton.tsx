import React, { useState } from 'react';
import { Button, Input, Modal } from 'antd';
import { DownloadOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import { EntityType, FacetFilterInput, SearchAcrossEntitiesInput } from '../../../../../../types.generated';
import { SearchResultsInterface } from './types';
import { getSearchCsvDownloadHeader, transformResultsToCsvRow } from './downloadAsCsvUtil';
import { downloadRowsAsCsv } from '../../../../../search/utils/csvUtils';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { useEntityData } from '../../../EntityContext';

const DownloadCsvButton = styled(Button)`
    font-size: 12px;
    padding-left: 12px;
    padding-right: 12px;
`;

type Props = {
    callSearchOnVariables: (variables: {
        input: SearchAcrossEntitiesInput;
    }) => Promise<SearchResultsInterface | null | undefined>;
    entityFilters: EntityType[];
    filters: FacetFilterInput[];
    query: string;
};

const SEARCH_PAGE_SIZE_FOR_DOWNLOAD = 1000;

export default function DownloadAsCsvButton({ callSearchOnVariables, entityFilters, filters, query }: Props) {
    const { entityData: entitySearchIsEmbeddedWithin } = useEntityData();

    const [isDownloadingCsv, setIsDownloadingCsv] = useState(false);
    const [showSaveAsModal, setShowSaveAsModal] = useState(false);
    const [saveAsTitle, setSaveAsTitle] = useState(
        entitySearchIsEmbeddedWithin ? `${entitySearchIsEmbeddedWithin.name}_impact.csv` : 'results.csv',
    );
    const entityRegistry = useEntityRegistry();

    const triggerCsvDownload = (filename) => {
        setIsDownloadingCsv(true);
        console.log('preparing your csv');

        let downloadPage = 0;
        let accumulatedResults: string[][] = [];

        function fetchNextPage() {
            console.log('fetch page number ', downloadPage);
            callSearchOnVariables({
                input: {
                    types: entityFilters,
                    query,
                    start: SEARCH_PAGE_SIZE_FOR_DOWNLOAD * downloadPage,
                    count: SEARCH_PAGE_SIZE_FOR_DOWNLOAD,
                    filters,
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
        <>
            <DownloadCsvButton type="text" onClick={() => setShowSaveAsModal(true)} disabled={isDownloadingCsv}>
                <DownloadOutlined />
                {isDownloadingCsv ? 'Downloading...' : 'Download'}
            </DownloadCsvButton>
            <Modal
                title="Download as..."
                visible={showSaveAsModal}
                footer={
                    <>
                        <Button onClick={() => setShowSaveAsModal(false)} type="text">
                            Close
                        </Button>
                        <Button
                            onClick={() => {
                                setShowSaveAsModal(false);
                                triggerCsvDownload(saveAsTitle);
                            }}
                            disabled={saveAsTitle.length === 0}
                        >
                            Download
                        </Button>
                    </>
                }
            >
                <Input placeholder="example.csv" value={saveAsTitle} onChange={(e) => setSaveAsTitle(e.target.value)} />
            </Modal>
        </>
    );
}
