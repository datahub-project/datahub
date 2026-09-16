import { borders, radius } from '@components';
import styled from 'styled-components';

const ModuleContainer = styled.div<{ $height?: string; $isDragging?: boolean }>`
    background: ${(props) => props.theme.colors.bg};
    border: ${borders['1px']} ${(props) => props.theme.colors.border};
    border-radius: ${radius.lg};
    flex: 1;
    overflow-x: hidden;
    box-shadow: ${(props) => props.theme.colors.shadowXs};
    opacity: 1;
    transition: opacity 0.2s ease;

    ${(props) =>
        props.$isDragging &&
        `
        background: ${props.theme.colors.bgSurface};
        box-shadow: ${props.theme.colors.shadowSm};
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
