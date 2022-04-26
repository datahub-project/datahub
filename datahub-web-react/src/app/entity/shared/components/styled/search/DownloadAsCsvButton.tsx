import React from 'react';
import { Button } from 'antd';
import { DownloadOutlined } from '@ant-design/icons';
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
    return (
        <>
            <DownloadCsvButton type="text" onClick={() => setShowDownloadAsCsvModal(true)} disabled={isDownloadingCsv}>
                <DownloadOutlined />
                {isDownloadingCsv ? 'Downloading...' : 'Download'}
            </DownloadCsvButton>
        </>
    );
}
