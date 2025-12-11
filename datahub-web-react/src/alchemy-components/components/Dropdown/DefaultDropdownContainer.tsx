/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
