import React, { useMemo } from 'react';
import styled from 'styled-components';

import { getFileIconFromExtension } from '@components/components/FileNode/utils';
import { Icon } from '@components/components/Icon';

const StyledIcon = styled(Icon)`
    flex-shrink: 0;
`;

interface Props {
    extension?: string;
    className?: string;
}

export function FileIcon({ extension, className }: Props) {
    const icon = useMemo(() => getFileIconFromExtension(extension || ''), [extension]);

    return <StyledIcon icon={icon} size="lg" source="phosphor" color="primary" className={className} />;
}
