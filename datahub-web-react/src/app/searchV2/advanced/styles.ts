import styled from 'styled-components';

export const FilterContainer = styled.div<{ $isCompact: boolean; isDisabled?: boolean }>`
    box-shadow: ${(props) => props.theme.colors.shadowXs};
    border-radius: 10px;
    border: 1px solid ${(props) => props.theme.colors.bgSurface};
    padding: ${(props) => (props.$isCompact ? '0 4px' : '4px')};
    margin: ${(props) => (props.$isCompact ? '2px 4px 2px 4px' : '4px')};

    ${(props) =>
        props.isDisabled
            ? `background: ${props.theme.colors.bgSurface};`
            : `
            :hover {
                cursor: pointer;
                background: ${props.theme.colors.bgSurface};
            }
    `}
`;
