import styled from 'styled-components';

export const ContentText = styled.span`
    font-size: 12px;
    font-weight: 500;
    color: ${(props) => props.theme.colors.textSecondary};
`;

export const LabelText = styled.span`
    font-size: 12px;
    font-weight: 500;
    color: ${(props) => props.theme.colors.textSecondary};
`;

export const InstanceIcon = styled.div`
    height: 22px;
    width: 22px;
    background-color: ${(props) => props.theme.colors.bgSurfaceSuccess};
    border-radius: 6px;
    display: flex;
    align-items: center;
    justify-content: center;
    svg {
        padding: 3px;
        height: 20px;
        width: 20px;
    }
`;

export const StyledLabel = styled.span`
    font-size: 16px;
    font-weight: 400;
    color: ${(props) => props.theme.colors.textSecondary};
`;
