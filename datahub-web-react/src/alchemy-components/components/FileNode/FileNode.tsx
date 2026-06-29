import { Button } from '@components';
import { X } from '@phosphor-icons/react/dist/csr/X';
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { getExtensionFromFileName } from '@components/components/Editor/extensions/fileDragDrop/fileUtils';
import { FileIcon } from '@components/components/FileNode/FileIcon';
import { FileNodeProps } from '@components/components/FileNode/types';
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

const FileName = styled.span`
    color: ${({ theme }) => theme.colors.textBrand};
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    display: block;
    min-width: 0;
    margin: 0;
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

    const hasRightContent = useMemo(() => {
        return !!onClose || !!extraRightContent;
    }, [onClose, extraRightContent]);

    const fontSize = useMemo(() => getFontSize(size), [size]);

    const fileNameRef = useRef<HTMLSpanElement>(null);
    const [isTruncated, setIsTruncated] = useState(false);
    useEffect(() => {
        const el = fileNameRef.current;
        if (el) setIsTruncated(el.scrollWidth > el.clientWidth);
    }, [fileName, loading, t]);

    if (!fileName) return null;

    if (loading) {
        return (
            <Container $border={border} className={className} $fontSize={fontSize}>
                <FileDetails>
                    <Loading height={18} width={20} marginTop={0} />
                    <Tooltip title={isTruncated ? fileName : undefined}>
                        <FileName ref={fileNameRef}>{t('fileNode.uploading', { fileName })}</FileName>
                    </Tooltip>
                </FileDetails>
            </Container>
        );
    }

    return (
        <Container $border={border} className={className} $fontSize={fontSize}>
            <FileDetails onClick={clickHandler}>
                <FileIcon extension={extension} />
                <Tooltip title={isTruncated ? fileName : undefined}>
                    <FileName ref={fileNameRef}>{fileName}</FileName>
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
