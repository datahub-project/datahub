import styled from 'styled-components';

export const BarContainer = styled.div`
    display: flex;
    gap: 2px;
    align-items: baseline;
`;

export const IndividualBar = styled.div<{ height: number; isColored: boolean; color: string; size: string }>`
    width: ${(props) => (props.size === 'default' ? '5px' : '3px')};
    height: ${(props) => props.height}px;
    background-color: ${(props) => (props.isColored ? props.color : '#C6C0E0')};
    border-radius: 20px;
    transition: background-color 0.3s ease, height 0.3s ease;
`;
