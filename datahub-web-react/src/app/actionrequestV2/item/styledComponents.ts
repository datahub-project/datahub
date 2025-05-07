import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { colors } from '@src/alchemy-components';

export const ContentWrapper = styled.div`
    font-size: 14px;
    display: inline-flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 6px;
`;

export const StyledLink = styled(Link)`
    color: ${colors.violet[500]};
    font-weight: 500;

    :hover {
        color: ${colors.violet[500]};
        text-decoration: underline;
    }
`;
