import { spacing } from '@components/theme';
import styled from 'styled-components';

import { PillStyleProps } from './types';
import { getPillStyle } from './utils';

export const PillContainer = styled.div(
    // Dynamic styles
    (props: PillStyleProps) => ({ ...getPillStyle(props as PillStyleProps) }),
    {
        // Base root styles
        display: 'inline-flex',
        alignItems: 'center',
        gap: spacing.xsm,
        cursor: 'pointer',
        padding: '0px 8px',
        borderRadius: '200px',

        // Base Disabled styles
        '&:disabled': {
            cursor: 'not-allowed',
        },
    },
);
