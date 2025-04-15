import { Button, Tooltip } from '@components';
import React from 'react';

type Props = {
    setShowDownloadAsCsvModal: (showDownloadAsCsvModal: boolean) => any;
    isDownloadingCsv: boolean;
    disabled?: boolean;
};

export default function DownloadButton({ setShowDownloadAsCsvModal, isDownloadingCsv, disabled }: Props) {
    return (
        <Tooltip title="Download results..." showArrow={false} placement="top">
            <Button
                onClick={() => setShowDownloadAsCsvModal(true)}
                disabled={isDownloadingCsv || disabled}
                color="gray"
                size="sm"
                isCircle
                icon={{ icon: 'DownloadSimple', source: 'phosphor', size: 'md' }}
            >
                {isDownloadingCsv ? 'Downloading...' : null}
            </Button>
        </Tooltip>
    );
}
