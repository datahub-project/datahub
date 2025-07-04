import { colors, radius, shadows, spacing, transition, zIndices } from "@components/theme";
import styled from "styled-components";

const DefaultDropdownContainer = styled.div(({
    borderRadius: radius.md,
    background: colors.white,
    zIndex: zIndices.dropdown,
    transition: `${transition.property.colors} ${transition.easing['ease-in-out']} ${transition.duration.normal}`,
    boxShadow: shadows.dropdown,
    padding: spacing.xsm,
    marginTop: '4px',
    overflow: 'auto',
    width: '100%',
}));

export default DefaultDropdownContainer;