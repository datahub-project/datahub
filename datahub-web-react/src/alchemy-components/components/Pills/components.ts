import styled from 'styled-components';

import { PillStyleProps } from '@components/components/Pills/types';
import { getPillStyle } from '@components/components/Pills/utils';
import { spacing } from '@components/theme';
import { SizeOptions } from '@components/theme/config';

const ICON_HIT_AREA: Record<SizeOptions, string> = {
    xs: '16px',
    sm: '22px',
    md: '24px',
    lg: '30px',
    xl: '34px',
    inherit: '1em',
};

const PILL_PADDING_X = 8;
const PILL_PADDING_X_WITH_ICON = 4;

type PillContainerProps = PillStyleProps & {
    $hasLeftIcon?: boolean;
    $hasRightIcon?: boolean;
};

export const PillContainer = styled.div<PillContainerProps>(
    ({ $hasLeftIcon, $hasRightIcon }) => ({
        // Base root styles
        display: 'inline-flex',
        alignItems: 'center',
        gap: spacing.xxsm,
        cursor: 'pointer',
        // Tighten end padding when an icon sits on that side so the pill doesn't feel stretched
        padding: `0 ${$hasRightIcon ? PILL_PADDING_X_WITH_ICON : PILL_PADDING_X}px 0 ${
            $hasLeftIcon ? PILL_PADDING_X_WITH_ICON : PILL_PADDING_X
        }px`,
        borderRadius: '200px',
        maxWidth: '100%',

        // Keep icons matched to the pill text color and vertically centered
        '& svg': {
            color: 'currentColor',
            display: 'block',
        },

        // Base Disabled styles
        '&:disabled': {
            cursor: 'not-allowed',
        },
    }),
    // Dynamic styles
    (props) => ({ ...getPillStyle(props) }),
);

export const PillText = styled.span({
    maxWidth: '100%',
    display: 'block',
    whiteSpace: 'nowrap',
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    lineHeight: 'inherit',
});

const iconHitAreaStyles = ($size: SizeOptions) => ({
    display: 'inline-flex',
    alignItems: 'center',
    justifyContent: 'center',
    flexShrink: 0,
    width: ICON_HIT_AREA[$size],
    height: ICON_HIT_AREA[$size],
    padding: 0,
    margin: 0,
    border: 'none',
    background: 'transparent',
    color: 'inherit',
    lineHeight: 0,
});

/** Non-interactive icon slot — same footprint as the clickable hit area */
export const PillIconSlot = styled.span<{ $size: SizeOptions }>(({ $size }) => ({
    ...iconHitAreaStyles($size),
}));

/** Clickable icon hit area — sized to the pill line-height for an easy tap target */
export const PillIconButton = styled.button<{ $size: SizeOptions }>(({ $size }) => ({
    ...iconHitAreaStyles($size),
    cursor: 'pointer',

    '&:disabled': {
        cursor: 'not-allowed',
    },
}));
