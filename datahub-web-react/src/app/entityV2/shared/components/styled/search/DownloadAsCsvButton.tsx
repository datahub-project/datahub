import React from 'react';
import { Button } from 'antd';
import { DownloadOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import { THEME_COLOR_BLUE } from '../../../constants';

const DownloadCsvButton = styled(Button)`
    font-size: 12px;
    padding-left: 12px;
    padding-right: 12px;
    background-color: ${THEME_COLOR_BLUE};
    color: #ffffff;
    :hover {
        background-color: #ffffff;
        color: ${THEME_COLOR_BLUE};
        border: 1px solid ${THEME_COLOR_BLUE};
    }
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
                {isDownloadingCsv ? 'Downloading...' : null}
            </DownloadCsvButton>
        </>
    );
}
