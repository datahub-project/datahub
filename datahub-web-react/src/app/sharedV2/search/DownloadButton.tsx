import { Button, Tooltip } from '@components';
import React from 'react';

type Props = {
    setShowDownloadAsCsvModal: (showDownloadAsCsvModal: boolean) => any;
    isDownloadingCsv: boolean;
    disabled?: boolean;
};

export default function DownloadButton({ setShowDownloadAsCsvModal, isDownloadingCsv, disabled }: Props) {
    return (
        <Tooltip title="Download results...", showArrow={false} placement="top">
            <Button
                onClick={() => setShowDownloadAsCsvModal(true)}
                disabled={isDownloadingCsv || disabled}
                icon={{ icon: 'DownloadSimple', source: 'phosphor' }}
                variant="text"
                color="gray"
                size="md"
                data-testid="download-csv-button"
            >
                {isDownloadingCsv ? 'Downloading...' : null}
            </Button>
        </Tooltip>
    );
}
