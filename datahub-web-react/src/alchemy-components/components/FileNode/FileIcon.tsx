import React, { useMemo } from 'react';
import styled from 'styled-components';

import { getFileIconFromExtension } from '@components/components/FileNode/utils';
import { getColor } from '@components/theme/utils';

import { useCustomTheme } from '@src/customThemeContext';

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
    const { theme } = useCustomTheme();

    return (
        <IconWrapper className={className}>
            <IconComponent style={{ fontSize: '16px', color: getColor('primary', undefined, theme) }} />
        </IconWrapper>
    );
}
