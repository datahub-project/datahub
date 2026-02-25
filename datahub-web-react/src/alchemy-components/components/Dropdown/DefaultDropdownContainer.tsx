import styled from 'styled-components';

import { colors, radius, shadows, spacing, transition, zIndices } from '@components/theme';

const DefaultDropdownContainer = styled.div<{ maxHeight?: number }>(({ maxHeight }) => ({
    borderRadius: radius.md,
    background: colors.white,
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
