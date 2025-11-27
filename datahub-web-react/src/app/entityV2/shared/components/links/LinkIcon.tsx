import { FileIcon, Icon } from '@components';
import React, { useCallback } from 'react';
import styled from 'styled-components';

import { isFileUrl } from '@components/components/Editor/extensions/fileDragDrop';
import {
    getExtensionFromFileName,
    getFileNameFromUrl,
} from '@components/components/Editor/extensions/fileDragDrop/fileUtils';

import { useIsDocumentationFileUploadV1Enabled } from '@app/shared/hooks/useIsDocumentationFileUploadV1Enabled';

const Container = styled.div<{ $iconColor?: string; $iconSize?: number }>`
    display: inline-block;
    ${({ $iconColor }) => $iconColor && `color: ${$iconColor};`}
    ${({ $iconSize }) =>
        $iconSize &&
        `
        svg {
            width: ${$iconSize}px;
            height: ${$iconSize}px;
        }
    `}
`;

interface Props {
    url: string;
    className?: string;
    iconColor?: string;
    iconSize?: number;
    style?: React.CSSProperties;
}

export function LinkIcon({ url, className, iconColor, iconSize, style }: Props) {
    const isDocumentationFileUploadV1Enabled = useIsDocumentationFileUploadV1Enabled();

    const renderIcon = useCallback(() => {
        if (isDocumentationFileUploadV1Enabled && isFileUrl(url)) {
            const fileName = getFileNameFromUrl(url);
            const extension = getExtensionFromFileName(fileName || '');
            return <FileIcon extension={extension} />;
        }

        // Use gray color with level 600 if iconColor is provided, otherwise use primary
        const color = iconColor ? 'gray' : 'primary';

        return (
            <Icon
                icon="LinkSimple"
                source="phosphor"
                color={color}
                colorLevel={iconColor ? 600 : undefined}
                size="lg"
            />
        );
    }, [isDocumentationFileUploadV1Enabled, url, iconColor]);

    return (
        <Container className={className} style={style} $iconColor={iconColor} $iconSize={iconSize}>
            {renderIcon()}
        </Container>
    );
}
