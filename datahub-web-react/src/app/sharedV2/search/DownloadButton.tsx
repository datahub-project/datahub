import { DownloadOutlined } from '@ant-design/icons';
import { Button, Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components';

const StyledButton = styled(Button)`
    height: 28px;
    margin: 0px 4px 0px 4px;
`;

type Props = {
    setShowDownloadAsCsvModal: (showDownloadAsCsvModal: boolean) => any;
    isDownloadingCsv: boolean;
    disabled?: boolean;
};

export default function DownloadButton({ setShowDownloadAsCsvModal, isDownloadingCsv, disabled }: Props) {
    return (
        <Tooltip title="Download results..." showArrow={false} placement="top">
            <StyledButton onClick={() => setShowDownloadAsCsvModal(true)} disabled={isDownloadingCsv || disabled}>
                <DownloadOutlined />
                {isDownloadingCsv ? 'Downloading...' : null}
            </StyledButton>
        </Tooltip>
    );
}
