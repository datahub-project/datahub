import { Button, Tooltip } from '@components';
import { DownloadSimple } from '@phosphor-icons/react/dist/csr/DownloadSimple';
import React from 'react';
import { useTranslation } from 'react-i18next';

type Props = {
    setShowDownloadAsCsvModal: (showDownloadAsCsvModal: boolean) => any;
    isDownloadingCsv: boolean;
    disabled?: boolean;
};

export default function DownloadButton({ setShowDownloadAsCsvModal, isDownloadingCsv, disabled }: Props) {
    const { t } = useTranslation('shared.search');
    return (
        <Tooltip title={t('downloadResults.tooltip')} showArrow={false} placement="top">
            <Button
                onClick={() => setShowDownloadAsCsvModal(true)}
                disabled={isDownloadingCsv || disabled}
                isCircle
                icon={{ icon: DownloadSimple }}
                variant="text"
                color="gray"
                size="sm"
                data-testid="download-csv-button"
            >
                {isDownloadingCsv ? t('downloading') : null}
            </Button>
        </Tooltip>
    );
}
