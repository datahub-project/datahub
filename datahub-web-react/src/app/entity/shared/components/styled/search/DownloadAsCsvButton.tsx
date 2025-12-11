/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { DownloadOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React from 'react';
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
