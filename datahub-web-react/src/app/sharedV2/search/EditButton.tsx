import { Button, Tooltip } from '@components';
import { PencilSimple } from '@phosphor-icons/react/dist/csr/PencilSimple';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

const StyledButton = styled(Button)`
    border: 1px solid ${({ theme }) => theme.colors.border};
`;

type Props = {
    setShowSelectMode: (showSelectMode: boolean) => any;
    disabled?: boolean;
};

export default function EditButton({ setShowSelectMode, disabled }: Props) {
    const { t } = useTranslation('shared.search');
    return (
        <Tooltip title={t('edit.tooltip')} showArrow={false} placement="top">
            <StyledButton
                onClick={() => setShowSelectMode(true)}
                disabled={disabled}
                data-testid="search-results-edit-button"
                isCircle
                icon={{ icon: PencilSimple }}
                variant="text"
                color="gray"
                size="sm"
            />
        </Tooltip>
    );
}
