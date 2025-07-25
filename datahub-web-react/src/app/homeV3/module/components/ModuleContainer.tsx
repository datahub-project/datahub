import { borders, colors, radius } from '@components';
import styled from 'styled-components';

const ModuleContainer = styled.div<{ $height?: string; $isDragging?: boolean }>`
    background: ${colors.white};
    border: ${borders['1px']} ${colors.gray[100]};
    border-radius: ${radius.lg};
    flex: 1;
    overflow-x: hidden;
    box-shadow:
        0px 2px 18px 0px rgba(17, 7, 69, 0.01),
        0px 4px 12px 0px rgba(17, 7, 69, 0.03);
    opacity: 1;
    transition: opacity 0.2s ease;

    ${({ $isDragging }) =>
        $isDragging &&
        `
        background: linear-gradient(180deg, #f0f8ff 0%, #e6f3ff 100%);
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
        z-index: 1000;
        transform: translateZ(0) scale(1.02);
        opacity: 0.5;
    `}

    ${(props) =>
        props.$height &&
        `
            height: ${props.$height};
        `}
`;

export default ModuleContainer;
