import styled from 'styled-components';

export const GraphCardHeader = styled.div`
    display: flex;
    flex-direction: row;
    gap: 16px;
    justify-content: space-between;
`;

export const GraphCardBody = styled.div`
    width: 100%;
    position: relative;
`;

export const ControlsContainer = styled.div`
    height: 42px;
    display: flex;
    flex-direction: row;
    gap: 8px;
`;

export const GraphContainer = styled.div<{ $isEmpty?: boolean; $height: string }>`
    width: 100%;
    height: ${(props) => props.$height};

    ${(props) =>
        props.$isEmpty &&
        `
        position: relative;
        pointer-events: none;
        filter: blur(2px);  
    `}
`;

export const EmptyMessageContainer = styled.div`
    top: 50%;
    left: 50%;
    transform: translate(-50%, -50%);
    position: absolute;
`;

export const LoaderContainer = styled.div<{ $height: string }>`
    display: flex;
    width: 100%;
    height: ${(props) => props.$height};
    min-height: 200px;
`;
