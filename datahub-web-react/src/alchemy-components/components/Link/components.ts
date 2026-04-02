import styled from 'styled-components';

import { LinkStyleProps } from '@components/components/Link/types';
import { getColor } from '@components/theme/utils';

export const StyledLink = styled.a<LinkStyleProps>`
    color: ${(props) => {
        const semantic = props.theme.colors[props.color ?? ''];
        return typeof semantic === 'string' ? semantic : getColor(props.color, props.colorLevel, props.theme);
    }};
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
