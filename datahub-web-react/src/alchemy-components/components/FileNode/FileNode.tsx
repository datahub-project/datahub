import { Button, colors } from '@components';
import { Typography } from 'antd';
import React, { useCallback, useMemo } from 'react';
import styled from 'styled-components';

import {
    getExtensionFromFileName,
    getFileNameFromUrl,
} from '@components/components/Editor/extensions/fileDragDrop/fileUtils';
import { FileIcon } from '@components/components/FileNode/FileIcon';
import { FileNodeProps } from '@components/components/FileNode/types';
import { getFontSize } from '@components/theme/utils';

import Loading from '@app/shared/Loading';

const Container = styled.div<{ $border?: boolean; $fontSize?: string }>`
    display: flex;
    width: 100%;

    ${(props) =>
        props.$border &&
        `
        border-radius: 8px;
        border: 1px solid ${colors.gray[100]};
    `}

    ${(props) => props.$fontSize && `font-size: ${props.$fontSize};`}
`;

const FileDetails = styled.span`
    display: flex;
    gap: 4px;
    align-items: center;
    font-weight: 600;
    width: 100%;
    padding: 4px;
`;

const SpaceFiller = styled.div`
    flex-grow: 1;
`;

const CloseButton = styled(Button)`
    padding: 0;
`;

const FileName = styled(Typography.Text)`
    color: ${({ theme }) => theme.styles['primary-color']};
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
`;

export function FileNode({
    fileName,
    loading,
    border,
    extraRightContent,
    className,
    size,
    onClick,
    onClose,
}: FileNodeProps) {
    const extension = useMemo(() => getExtensionFromFileName(fileName || ''), [fileName]);

    const closeHandler = useCallback(
        (e: React.MouseEvent) => {
            e.preventDefault();
            e.stopPropagation();
            onClose?.(e);
        },
        [onClose],
    );

    const clickHandler = useCallback(
        (e: React.MouseEvent) => {
            e.preventDefault();
            e.stopPropagation();
            onClick?.(e);
        },
        [onClick],
    );

    const hasRightContent = useMemo(() => {
        return !!onClose || !!extraRightContent;
    }, [onClose, extraRightContent]);

    const fontSize = useMemo(() => getFontSize(size), [size]);

    if (!fileName) return null;

    if (loading) {
        return (
            <Container $border={border} className={className} $fontSize={fontSize}>
                <FileDetails>
                    <Loading height={18} width={20} marginTop={0} />
                    <FileName ellipsis={{ tooltip: name }}>Uploading {name}...</FileName>
                </FileDetails>
            </Container>
        );
    }

    return (
        <Container $border={border} className={className} $fontSize={fontSize}>
            <FileDetails onClick={clickHandler}>
                <FileIcon extension={extension} />
                <FileName ellipsis={{ tooltip: fileName }}>{fileName}</FileName>

                {hasRightContent && <SpaceFiller />}

                {onClose && (
                    <CloseButton
                        icon={{
                            icon: 'X',
                            source: 'phosphor',
                            color: 'gray',
                            colorLevel: 1800,
                            size: 'lg',
                        }}
                        variant="link"
                        onClick={closeHandler}
                    />
                )}
            </FileDetails>
            {extraRightContent}
        </Container>
    );
}
