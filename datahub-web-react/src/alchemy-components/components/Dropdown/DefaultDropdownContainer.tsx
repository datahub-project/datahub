import styled from 'styled-components';

import { radius, shadows, spacing, transition, zIndices } from '@components/theme';

const DefaultDropdownContainer = styled.div<{ maxHeight?: number }>(({ maxHeight, theme }) => ({
    borderRadius: radius.md,
    background: theme.colors.bg,
    zIndex: zIndices.dropdown,
    transition: `${transition.property.colors} ${transition.easing['ease-in-out']} ${transition.duration.normal}`,
    boxShadow: shadows.dropdown,
    padding: spacing.xsm,
    display: 'flex',
    flexDirection: 'column',
    gap: '8px',
    marginTop: '4px',
    overflow: 'auto',
    width: '100%',
    maxHeight,
}));

export default DefaultDropdownContainer;
