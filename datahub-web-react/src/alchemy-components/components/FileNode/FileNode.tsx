import { Button } from '@components';
import { X } from '@phosphor-icons/react/dist/csr/X';
import React, { useCallback, useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { getExtensionFromFileName } from '@components/components/Editor/extensions/fileDragDrop/fileUtils';
import { FileIcon } from '@components/components/FileNode/FileIcon';
import { FileNodeProps } from '@components/components/FileNode/types';
import { Text } from '@components/components/Text';
import { Tooltip } from '@components/components/Tooltip';
import { getFontSize } from '@components/theme/utils';

import Loading from '@app/shared/Loading';

const Container = styled.div<{ $border?: boolean; $fontSize?: string }>`
    display: flex;
    width: 100%;

    ${(props) =>
        props.$border &&
        `
        border-radius: 8px;
        border: 1px solid ${props.theme.colors.border};
    `}

    ${(props) => props.$fontSize && `font-size: ${props.$fontSize};`}
`;

const FileDetails = styled.div<{ $isClickable?: boolean }>`
    display: flex;
    gap: 4px;
    align-items: center;
    font-weight: 600;
    width: 100%;
    padding: 4px;
    ${({ $isClickable, theme }) =>
        $isClickable
            ? `
        cursor: pointer;

        &:focus-visible {
            outline: 2px solid ${theme.colors.borderBrandFocused};
            outline-offset: 2px;
        }
    `
            : ''}
`;

const SpaceFiller = styled.div`
    flex-grow: 1;
`;

const CloseButton = styled(Button)`
    padding: 0;
`;

const FileName = styled(Text)`
    color: ${({ theme }) => theme.colors.textBrand};
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    display: block;
    min-width: 0;
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
    const { t } = useTranslation('alchemy');
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

    const keyDownHandler = useCallback(
        (e: React.KeyboardEvent) => {
            if (!onClick) return;
            if (e.key === 'Enter' || e.key === ' ') {
                e.preventDefault();
                onClick(e as unknown as React.MouseEvent);
            }
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
                    <Tooltip title={fileName}>
                        <FileName type="span">{t('fileNode.uploading', { fileName })}</FileName>
                    </Tooltip>
                </FileDetails>
            </Container>
        );
    }

    return (
        <Container $border={border} className={className} $fontSize={fontSize}>
            <FileDetails
                $isClickable={!!onClick}
                onClick={clickHandler}
                role={onClick ? 'button' : undefined}
                tabIndex={onClick ? 0 : undefined}
                aria-label={fileName}
                onKeyDown={keyDownHandler}
            >
                <FileIcon extension={extension} />
                <Tooltip title={fileName}>
                    <FileName type="span">{fileName}</FileName>
                </Tooltip>

                {hasRightContent && <SpaceFiller />}

                {onClose && (
                    <CloseButton
                        icon={{
                            icon: X,
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
