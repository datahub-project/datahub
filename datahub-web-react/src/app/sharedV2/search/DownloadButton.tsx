import { Button, Tooltip, colors } from '@components';
import React from 'react';
import styled from 'styled-components';

const StyledButton = styled(Button)`
    border: 1px solid ${colors.gray[100]};
`;

type Props = {
    setShowDownloadAsCsvModal: (showDownloadAsCsvModal: boolean) => any;
    isDownloadingCsv: boolean;
    disabled?: boolean;
};

export default function DownloadButton({ setShowDownloadAsCsvModal, isDownloadingCsv, disabled }: Props) {
    return (
        <Tooltip title="Download results..." showArrow={false} placement="top">
            <StyledButton
                onClick={() => setShowDownloadAsCsvModal(true)}
                disabled={isDownloadingCsv || disabled}
                isCircle
                icon={{ icon: 'DownloadSimple', source: 'phosphor' }}
                variant="text"
                color="gray"
                size="sm"
                data-testid="download-csv-button"
            >
                {isDownloadingCsv ? 'Downloading...' : null}
            </StyledButton>
        </Tooltip>
    );
}
