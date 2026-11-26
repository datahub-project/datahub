import { Button, Tooltip } from '@components';
import { Funnel } from '@phosphor-icons/react/dist/csr/Funnel';
import { X } from '@phosphor-icons/react/dist/csr/X';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { radius } from '@components/theme';

const SelectButtonContainer = styled.div`
    display: flex;
    align-items: center;
    align-self: center;
    max-width: 160px;
`;

const SelectedViewGroup = styled.div`
    display: inline-flex;
    align-items: center;
    max-width: 160px;
    border-radius: ${radius.sm};
    background-color: ${({ theme }) => theme.colors.bgSurfaceBrand};
    color: ${({ theme }) => theme.colors.textBrand};

    &:hover {
        background-color: ${({ theme }) =>
            theme.colors.buttonSurfaceSecondaryHover ?? theme.colors.bgSurfaceBrandHover};
    }
`;

const SelectedLabel = styled.span`
    max-width: 100px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

const NameButton = styled(Button)`
    && {
        background: transparent;
        color: inherit;
        padding-right: 4px;

        &:hover {
            background: transparent;
            box-shadow: none;
        }
    }
`;

const ClearButton = styled(Button)`
    && {
        background: transparent;
        color: inherit;
        padding: 0 8px 0 2px;
        min-width: auto;

        &:hover {
            background: transparent;
            box-shadow: none;
        }
    }
`;

type Props = {
    selectedViewName: string;
    onClear: () => void;
    onClick?: () => void;
};

export function SelectedViewButton({ selectedViewName, onClear, onClick }: Props) {
    const { t } = useTranslation('entity.views');
    const isSelected = Boolean(selectedViewName);

    return (
        <SelectButtonContainer data-testid="views-button-container">
            <Tooltip showArrow={false} title={selectedViewName || t('viewSelect.buttonLabel')} placement="bottom">
                {isSelected ? (
                    <SelectedViewGroup>
                        <NameButton
                            type="button"
                            variant="secondary"
                            color="primary"
                            size="sm"
                            onClick={() => onClick?.()}
                            data-testid="views-button"
                        >
                            <SelectedLabel data-testid="views-icon">{selectedViewName}</SelectedLabel>
                        </NameButton>
                        <ClearButton
                            type="button"
                            variant="text"
                            color="primary"
                            size="sm"
                            icon={{ icon: X, size: 'md', weight: 'bold' }}
                            aria-label={t('viewSelect.clearView')}
                            data-testid="views-clear-button"
                            onClick={(e) => {
                                e.stopPropagation();
                                onClear();
                            }}
                        />
                    </SelectedViewGroup>
                ) : (
                    <Button
                        type="button"
                        variant="text"
                        color="gray"
                        size="sm"
                        icon={{ icon: Funnel }}
                        onClick={() => onClick?.()}
                        data-testid="views-button"
                    >
                        <span data-testid="views-icon">{t('viewSelect.buttonLabel')}</span>
                    </Button>
                )}
            </Tooltip>
        </SelectButtonContainer>
    );
}

/** @deprecated Prefer `SelectedViewButton` — kept for existing call sites. */
export const renderSelectedView = (props: Props) => <SelectedViewButton {...props} />;
