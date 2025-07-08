import { borders, colors, radius } from '@components';
import styled from 'styled-components';

const ModuleContainer = styled.div<{ $height: string }>`
    background: ${colors.white};
    border: ${borders['1px']} ${colors.gray[100]};
    border-radius: ${radius.lg};

    height: ${(props) => props.$height};
    box-shadow:
        0px 2px 18px 0px rgba(17, 7, 69, 0.01),
        0px 4px 12px 0px rgba(17, 7, 69, 0.03);
`;

export default ModuleContainer;
