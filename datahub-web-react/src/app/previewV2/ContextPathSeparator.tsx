import { colors } from '@components';
import React from 'react';
import styled from 'styled-components';

import { Icon } from '@components/components/Icon';

const StyledIcon = styled(Icon)`
    flex-shrink: 0;
    margin: 0 2px;
    color: ${colors.gray[200]};
`;

export default function ContextPathSeparator() {
    return <StyledIcon icon="CaretRight" source="phosphor" size="sm" />;
}
