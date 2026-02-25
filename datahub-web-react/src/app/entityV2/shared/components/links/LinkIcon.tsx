import { FileIcon, Icon } from '@components';
import React, { useCallback } from 'react';
import styled from 'styled-components';

import { isFileUrl } from '@components/components/Editor/extensions/fileDragDrop';
import {
    getExtensionFromFileName,
    getFileNameFromUrl,
} from '@components/components/Editor/extensions/fileDragDrop/fileUtils';

import { useIsDocumentationFileUploadV1Enabled } from '@app/shared/hooks/useIsDocumentationFileUploadV1Enabled';

const Container = styled.div`
    display: inline-block;
`;

interface Props {
    url: string;
    className?: string;
    /**
     * Whether to use primary color. Defaults to true (primary color).
     * If false, uses gray color with level 600.
     */
    usePrimaryColor?: boolean;
    style?: React.CSSProperties;
}

export function LinkIcon({ url, className, usePrimaryColor = true, style }: Props) {
    const isDocumentationFileUploadV1Enabled = useIsDocumentationFileUploadV1Enabled();

    const renderIcon = useCallback(() => {
        if (isDocumentationFileUploadV1Enabled && isFileUrl(url)) {
            const fileName = getFileNameFromUrl(url);
            const extension = getExtensionFromFileName(fileName || '');
            return <FileIcon extension={extension} />;
        }

        return (
            <Icon
                icon="LinkSimple"
                source="phosphor"
                color={usePrimaryColor ? 'primary' : 'gray'}
                colorLevel={usePrimaryColor ? undefined : 600}
                size="lg"
            />
        );
    }, [isDocumentationFileUploadV1Enabled, url, usePrimaryColor]);

    return (
        <Container className={className} style={style}>
            {renderIcon()}
        </Container>
    );
}
