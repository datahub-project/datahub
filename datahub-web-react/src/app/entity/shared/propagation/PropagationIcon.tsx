import { Lightning } from '@phosphor-icons/react/dist/csr/Lightning';
import styled from 'styled-components';

/** Resting state of the propagation indicator: muted until the row is hovered. */
export const PropagateThunderbolt = styled(Lightning).attrs({ weight: 'fill', size: 16 })`
    color: ${(props) => props.theme.colors.icon};
    &:hover {
        color: ${(props) => props.theme.colors.iconInformation};
    }
    margin-right: 4px;
`;

/** Emphasised variant used inside the propagation popover's own title. */
export const PropagateThunderboltFilled = styled(Lightning).attrs({ weight: 'fill', size: 16 })`
    color: ${(props) => props.theme.colors.iconInformation};
    margin-right: 4px;
`;
