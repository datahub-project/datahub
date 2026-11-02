import styled from 'styled-components';

export const ControlPanel = styled.div`
    display: flex;
    flex-direction: column;
    align-items: flex-start;

    background: ${(props) => props.theme.colors.bg};
    border-radius: 8px;
    border: 1px solid ${(props) => props.theme.colors.border};
    box-shadow: ${(props) => props.theme.colors.shadowSm};
    padding: 16px;
    gap: 2px;

    height: fit-content;
    max-width: 255px;
    overflow: hidden;
`;

export const ControlPanelTitle = styled.div`
    font-size: 12px;
    font-weight: 700;
    line-height: 16px;
    color: ${(props) => props.theme.colors.text};
`;

export const ControlPanelSubtext = styled.div`
    font-size: 12px;
    font-weight: 400;
    line-height: 16px;
    color: ${(props) => props.theme.colors.text};
    margin-bottom: 8px;
`;
