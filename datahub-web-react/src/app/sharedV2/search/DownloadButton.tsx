import { DownloadOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import { Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components';
import { SEARCH_COLORS } from '../../entityV2/shared/constants';

const DownloadCsvButton = styled(Button)`
    font-size: 8px;
    padding-left: 12px;
    padding-right: 12px;
    background-color: ${SEARCH_COLORS.TITLE_PURPLE};
    color: #ffffff;
    :hover {
        background-color: #ffffff;
        color: ${SEARCH_COLORS.TITLE_PURPLE};
        border: 1px solid ${SEARCH_COLORS.TITLE_PURPLE};
    }
    height: 28px;
    margin: 0px 8px;
`;

type Props = {
    setShowDownloadAsCsvModal: (showDownloadAsCsvModal: boolean) => any;
    isDownloadingCsv: boolean;
    disabled?: boolean;
};

export default function DownloadButton({ setShowDownloadAsCsvModal, isDownloadingCsv, disabled }: Props) {
    return (
        <Tooltip title="Download results..." showArrow={false} placement="top">
            <DownloadCsvButton
                type="text"
                onClick={() => setShowDownloadAsCsvModal(true)}
                disabled={isDownloadingCsv || disabled}
            >
                <DownloadOutlined />
                {isDownloadingCsv ? 'Downloading...' : null}
            </DownloadCsvButton>
        </Tooltip>
    );
}
