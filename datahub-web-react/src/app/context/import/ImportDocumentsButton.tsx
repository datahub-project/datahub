import { Button, Tooltip } from '@components';
import { DownloadSimple } from '@phosphor-icons/react/dist/csr/DownloadSimple';
import React, { useState } from 'react';
import styled from 'styled-components';

import ImportDocumentsModal from '@app/context/import/ImportDocumentsModal';
import { ImportUseCase } from '@app/context/import/import.types';

const StyledButton = styled(Button)`
    padding: 2px;
    svg {
        width: 20px;
        height: 20px;
    }
`;

type ImportDocumentsButtonProps = {
    useCase?: ImportUseCase;
    onSuccess?: (parentUrn: string | null) => void;
    defaultParentUrn?: string | null;
};

export default function ImportDocumentsButton({
    useCase = ImportUseCase.CONTEXT_DOCUMENT,
    onSuccess,
    defaultParentUrn,
}: ImportDocumentsButtonProps) {
    const [isOpen, setIsOpen] = useState(false);

    return (
        <>
            <Tooltip title="Import Documents" placement="bottom" showArrow={false}>
                <span style={{ display: 'inline-block' }}>
                    <StyledButton
                        variant="filled"
                        color="primary"
                        isCircle
                        icon={{ icon: DownloadSimple }}
                        onClick={() => setIsOpen(true)}
                        data-testid="import-documents-button"
                    />
                </span>
            </Tooltip>
            <ImportDocumentsModal
                visible={isOpen}
                onClose={() => setIsOpen(false)}
                useCase={useCase}
                onSuccess={onSuccess}
                defaultParentUrn={defaultParentUrn}
            />
        </>
    );
}
