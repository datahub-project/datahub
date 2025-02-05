import { spacing } from '@components/theme';
import styled from 'styled-components';
import { PillStyleProps } from './types';

import { getPillStyle } from './utils';

export const PillContainer = styled.div(
    {
        // Base root styles
        display: 'inline-flex',
        alignItems: 'center',
        gap: spacing.xxsm,
        cursor: 'pointer',
        padding: '0px 8px',
        borderRadius: '200px',
        maxWidth: '100%',

        // Base Disabled styles
        '&:disabled': {
            cursor: 'not-allowed',
        },
    },
    // Dynamic styles
    (props: PillStyleProps) => ({ ...getPillStyle(props) }),
);

export const PillText = styled.span({
    maxWidth: '100%',
    display: 'block',
    whiteSpace: 'nowrap',
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    fontSize: '12px',
    fontWeight: 400,
});
