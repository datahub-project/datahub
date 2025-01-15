import styled from 'styled-components';

import { spacing } from '@components/theme';
import { ButtonProps } from './types';
import { getButtonStyle } from './utils';

export const ButtonBase = styled.button(
    // Dynamic styles
    (props: ButtonProps) => ({ ...getButtonStyle(props as ButtonProps) }),
    {
        // Base root styles
        display: 'flex',
        alignItems: 'center',
        gap: spacing.xsm,
        cursor: 'pointer',
        transition: `all 0.15s ease`,

        // For transitions between focus/active and hover states
        outlineColor: 'transparent',
        outlineStyle: 'solid',

        // Base Disabled styles
        '&:disabled': {
            cursor: 'not-allowed',
        },
    },
);
