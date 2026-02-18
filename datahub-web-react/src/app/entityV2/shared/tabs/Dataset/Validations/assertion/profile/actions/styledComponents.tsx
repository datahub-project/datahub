import styled from 'styled-components';

export const ActionItemButton = styled.button<{ disabled?: boolean; primary?: boolean; isExpandedView?: boolean }>`
    border-radius: ${(props) => (props.isExpandedView ? `0px` : `20px`)};
    width: 28px;
    height: 28px;
    margin: ${(props) => (props.isExpandedView ? `0px` : `0px 4px`)};
    padding: 0px;
    display: flex;
    align-items: center;
    justify-content: center;
    overflow: hidden;
    background-color: ${(props) => {
        if (props.isExpandedView) return 'inherit';
        return props.primary ? props.theme.colors.buttonFillBrand : props.theme.colors.bg;
    }};
    color: ${(props) => {
        if (props.primary) return props.theme.colors.textOnFillBrand;
        return props.isExpandedView ? props.theme.colors.text : props.theme.colors.textBrand;
    }};
    box-shadow: none;
    cursor: ${(props) => (props.disabled ? 'not-allowed' : 'pointer')};
    &&:hover {
        ${(props) => !props.disabled && 'opacity: 0.8;'}
    }
    ${(props) => props.disabled && 'opacity: 0.5'};
    border: ${(props) => {
        if (props.isExpandedView) return 'none';
        return props.primary ? `1px solid ${props.theme.colors.borderBrand}` : `1px solid ${props.theme.colors.border}`;
    }};
`;
