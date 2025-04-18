import styled from 'styled-components';

import { ButtonStyleProps } from '@components/components/Button/types';
import { getButtonStyle } from '@components/components/Button/utils';
import { spacing } from '@components/theme';

export const ButtonBase = styled.button(
    // Dynamic styles
    (props: ButtonStyleProps) => ({ ...getButtonStyle(props) }),
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
