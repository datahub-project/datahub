import React, { useState } from 'react';
import styled from 'styled-components';

import DownloadAsCsvModal from '@app/entityV2/shared/components/styled/search/DownloadAsCsvModal';
import { DownloadSearchResults, DownloadSearchResultsInput } from '@app/searchV2/utils/types';
import DownloadButton from '@app/sharedV2/search/DownloadButton';
import EditButton from '@app/sharedV2/search/EditButton';

import { AndFilterInput } from '@types';

const ButtonContainer = styled.div`
    display: flex;
    gap: 8px;
    align-items: center;
`;

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
            <ButtonContainer>
                <DownloadButton
                    disabled={totalResultsIsZero}
                    isDownloadingCsv={isDownloadingCsv}
                    setShowDownloadAsCsvModal={setShowDownloadAsCsvModal}
                />
                {setShowSelectMode && (
                    <EditButton setShowSelectMode={setShowSelectMode} disabled={totalResultsIsZero} />
                )}
            </ButtonContainer>
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
