<<<<<<< HEAD
import { Button } from '@components';
import React from 'react';
import styled from 'styled-components';

import { SelectSizeOptions } from '@components/components/Select/types';

import { colors, spacing } from '@src/alchemy-components/theme';
=======
import React from 'react';
import { Button } from '@components';
import { colors, spacing } from '@src/alchemy-components/theme';
import styled from 'styled-components';
import { SelectSizeOptions } from '../types';
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105

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
