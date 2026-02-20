import styled from 'styled-components';

export const ControlPanel = styled.div`
    display: flex;
    flex-direction: column;
    align-items: flex-start;

    background: ${(props) => props.theme.colors.bgSurface};
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
    font-size: 14px;
    font-weight: 700;
    color: ${(props) => props.theme.colors.textSecondary};
`;

export const ControlPanelSubtext = styled.div`
    color: ${(props) => props.theme.colors.textTertiary};
    font-size: 10px;
    font-weight: 500;
    margin-bottom: 8px;
`;
