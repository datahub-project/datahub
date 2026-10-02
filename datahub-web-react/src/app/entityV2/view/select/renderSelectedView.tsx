import { Button, Tooltip } from '@components';
import { Funnel } from '@phosphor-icons/react/dist/csr/Funnel';
import { X } from '@phosphor-icons/react/dist/csr/X';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

const SelectButtonContainer = styled.div`
    display: flex;
    align-items: center;
    align-self: center;
    max-width: 160px;
`;

const SelectedLabel = styled.span`
    max-width: 100px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

const ClearIconButton = styled.span`
    display: inline-flex;
    align-items: center;
    margin-left: 2px;
    line-height: 0;
    cursor: pointer;

    svg {
        display: block;
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
                    <Button
                        type="button"
                        variant="secondary"
                        color="primary"
                        size="sm"
                        onClick={() => onClick?.()}
                        data-testid="views-button"
                    >
                        <SelectedLabel data-testid="views-icon">{selectedViewName}</SelectedLabel>
                        <ClearIconButton
                            role="button"
                            tabIndex={0}
                            aria-label={t('viewSelect.clearView')}
                            data-testid="views-clear-button"
                            onClick={(e) => {
                                e.stopPropagation();
                                onClear();
                            }}
                            onKeyDown={(e) => {
                                if (e.key === 'Enter' || e.key === ' ') {
                                    e.preventDefault();
                                    e.stopPropagation();
                                    onClear();
                                }
                            }}
                        >
                            <X size={12} weight="bold" />
                        </ClearIconButton>
                    </Button>
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
                        {t('viewSelect.buttonLabel')}
                    </Button>
                )}
            </Tooltip>
        </SelectButtonContainer>
    );
}

/** @deprecated Prefer `SelectedViewButton` — kept for existing call sites. */
export const renderSelectedView = (props: Props) => <SelectedViewButton {...props} />;
