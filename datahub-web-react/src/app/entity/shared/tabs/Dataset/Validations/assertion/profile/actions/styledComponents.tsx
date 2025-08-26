import styled from 'styled-components';

export const ActionItemButton = styled.button<{ disabled?: boolean; primary?: boolean }>`
    border-radius: 20px;
    width: 28px;
    height: 28px;
    margin: 0px 4px;
    padding: 0px;
    display: flex;
    align-items: center;
    justify-content: center;
    overflow: hidden;
    border: none;
    background-color: ${(props) => (props.primary ? `#5280e8` : `#ffffff`)};
    border: 1px solid ${(props) => (props.primary ? `#5280e8` : `#f0f0f0`)};
    color: ${(props) => (props.primary ? `#fff` : `#5280e8`)};
    box-shadow: none;
    cursor: ${(props) => (props.disabled ? 'not-allowed' : 'pointer')};
    &&:hover {
        ${(props) => !props.disabled && 'opacity: 0.8;'}
    }
    ${(props) => props.disabled && 'opacity: 0.5'};
`;
