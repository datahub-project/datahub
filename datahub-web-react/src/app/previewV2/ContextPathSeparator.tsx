import { Icon } from '@components';
import React from 'react';
import styled from 'styled-components';

const StyledIcon = styled(Icon)`
    flex-shrink: 0;
    margin: 0 2px;
    color: ${(props) => props.theme.colors.border};
`;

export default function ContextPathSeparator() {
    return <StyledIcon icon="CaretRight" source="phosphor" size="sm" />;
}
