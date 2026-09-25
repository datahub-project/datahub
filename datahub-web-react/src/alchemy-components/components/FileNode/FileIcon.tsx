import React, { useMemo } from 'react';
import styled, { useTheme } from 'styled-components';

import { getFileIconFromExtension } from '@components/components/FileNode/utils';

const IconWrapper = styled.div`
    flex-shrink: 0;
    display: flex;
    align-items: center;
    font-size: 16px;
`;

interface Props {
    extension?: string;
    className?: string;
}

export function FileIcon({ extension, className }: Props) {
    const IconComponent = useMemo(() => getFileIconFromExtension(extension || ''), [extension]);
    const theme = useTheme();

    return (
        <IconWrapper className={className}>
            <IconComponent style={{ fontSize: '16px', color: theme.colors.iconBrand }} />
        </IconWrapper>
    );
}
