import { Link } from 'react-router-dom';
import styled from 'styled-components';

export const ContentWrapper = styled.div`
    font-size: 14px;
    display: inline-flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 6px;
`;

export const StyledLink = styled(Link)`
    color: ${(props) => props.theme.styles['primary-color']};
    font-weight: 500;

    :hover {
        color: ${(props) => props.theme.styles['primary-color']};
        text-decoration: underline;
    }
`;
