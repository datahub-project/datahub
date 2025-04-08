import { Button } from '@components';
import React from 'react';
import styled from 'styled-components';

import { SelectSizeOptions } from '@components/components/Select/types';

import { colors, spacing } from '@src/alchemy-components/theme';

const FooterBase = styled.div({
    display: 'flex',
    justifyContent: 'flex-end',
    gap: spacing.sm,
    paddingTop: spacing.sm,
    borderTop: `1px solid ${colors.gray[100]}`,
});

interface Props {
    onCancel?: () => void;
    onUpdate?: () => void;
    size?: SelectSizeOptions;
}

export default function DropdownFooterActions({ onCancel, onUpdate, size }: Props) {
    return (
        <FooterBase>
            <Button onClick={onCancel} variant="text" size={size}>
                Cancel
            </Button>
            <Button onClick={onUpdate} size={size} onFocus={(e) => e.stopPropagation()}>
                Update
            </Button>
        </FooterBase>
    );
}
