import { borders, radius } from '@components';
import styled from 'styled-components';

const ModuleContainer = styled.div<{ $height?: string; $isDragging?: boolean }>`
    background: ${(props) => props.theme.colors.bg};
    border: ${borders['1px']} ${(props) => props.theme.colors.border};
    border-radius: ${radius.lg};
    flex: 1;
    overflow-x: hidden;
    box-shadow:
        0px 2px 18px 0px rgba(17, 7, 69, 0.01),
        0px 4px 12px 0px rgba(17, 7, 69, 0.03);
    opacity: 1;
    transition: opacity 0.2s ease;

    ${(props) =>
        props.$isDragging &&
        `
        background: ${props.theme.colors.bgSurface};
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
