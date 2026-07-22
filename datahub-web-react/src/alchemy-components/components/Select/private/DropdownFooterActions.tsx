import { Button } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { SelectSizeOptions } from '@components/components/Select/types';

import { spacing } from '@src/alchemy-components/theme';

const FooterBase = styled.div(({ theme }) => ({
    display: 'flex',
    justifyContent: 'flex-end',
    gap: spacing.sm,
    paddingTop: spacing.sm,
    borderTop: `1px solid ${theme.colors.border}`,
}));

interface Props {
    onCancel?: () => void;
    onUpdate?: () => void;
    size?: SelectSizeOptions;
}

export default function DropdownFooterActions({ onCancel, onUpdate, size }: Props) {
    const { t: tc } = useTranslation('common.actions');
    return (
        <FooterBase>
            <Button onClick={onCancel} variant="text" size={size} data-testid="footer-button-cancel">
                {tc('cancel')}
            </Button>
            <Button
                onClick={onUpdate}
                size={size}
                onFocus={(e) => e.stopPropagation()}
                data-testid="footer-button-update"
            >
                {tc('update')}
            </Button>
        </FooterBase>
    );
}
