import styled from 'styled-components';

import { LinkStyleProps } from '@components/components/Link/types';
import { getColor } from '@components/theme/utils';

export const StyledLink = styled.a<LinkStyleProps>`
    color: ${(props) => getColor(props.color, props.colorLevel)};
    text-decoration: none;
    cursor: pointer;
    transition: opacity 0.2s ease;

    &:hover {
        opacity: 0.8;
        text-decoration: underline;
    }

    &:active {
        opacity: 0.6;
    }
`;
