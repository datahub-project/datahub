import React, { useState } from 'react';
import { AndFilterInput } from '../../../types.generated';
import { DownloadSearchResults, DownloadSearchResultsInput } from '../../searchV2/utils/types';
import DownloadAsCsvModal from '../../entityV2/shared/components/styled/search/DownloadAsCsvModal';
import EditButton from './EditButton';
import DownloadButton from './DownloadButton';

type Props = {
    filters: AndFilterInput[];
    query: string;
    viewUrn?: string;
    totalResults?: number;
    setShowSelectMode?: (showSelectMode: boolean) => any;
    downloadSearchResults: (input: DownloadSearchResultsInput) => Promise<DownloadSearchResults | null | undefined>;
};

export default function SearchMenuItems({
    filters,
    query,
    viewUrn,
    totalResults,
    setShowSelectMode,
    downloadSearchResults,
}: Props) {
    const [isDownloadingCsv, setIsDownloadingCsv] = useState(false);
    const [showDownloadAsCsvModal, setShowDownloadAsCsvModal] = useState(false);

    const totalResultsIsZero = totalResults === 0 || totalResults === undefined || totalResults === null;

    return (
        <>
            <DownloadButton
                disabled={totalResultsIsZero}
                isDownloadingCsv={isDownloadingCsv}
                setShowDownloadAsCsvModal={setShowDownloadAsCsvModal}
            />
            {setShowSelectMode && <EditButton setShowSelectMode={setShowSelectMode} disabled={totalResultsIsZero} />}
            <DownloadAsCsvModal
                downloadSearchResults={downloadSearchResults}
                filters={filters}
                query={query}
                viewUrn={viewUrn}
                setIsDownloadingCsv={setIsDownloadingCsv}
                showDownloadAsCsvModal={showDownloadAsCsvModal}
                setShowDownloadAsCsvModal={setShowDownloadAsCsvModal}
                totalResults={totalResults}
            />
        </>
    );
}
