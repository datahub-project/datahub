import { DownloadOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

const DownloadCsvButton = styled(Button)`
    font-size: 12px;
    padding-left: 12px;
    padding-right: 12px;
`;

type Props = {
    setShowDownloadAsCsvModal: (showDownloadAsCsvModal: boolean) => any;
    isDownloadingCsv: boolean;
};

export default function DownloadAsCsvButton({ setShowDownloadAsCsvModal, isDownloadingCsv }: Props) {
    const { t } = useTranslation('entityV1.shared.components');
    const { t: tc } = useTranslation('common.actions');
    return (
        <>
            <DownloadCsvButton type="text" onClick={() => setShowDownloadAsCsvModal(true)} disabled={isDownloadingCsv}>
                <DownloadOutlined />
                {isDownloadingCsv ? t('downloadCsv.downloading') : tc('download')}
            </DownloadCsvButton>
        </>
    );
}
