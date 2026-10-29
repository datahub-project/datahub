import { Handle, Position } from 'reactflow';
import styled from 'styled-components';

const HorizontalHandle = styled(Handle)<{ position: Position }>`
    background: initial;
    border: initial;
    ${({ position }) => (position === Position.Left ? 'left: 0;' : 'right: 0;')}
`;

export default HorizontalHandle;
