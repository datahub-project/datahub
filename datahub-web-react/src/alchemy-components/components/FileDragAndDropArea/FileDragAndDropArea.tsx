import { Button, Icon, Text, colors } from '@components';
import React, { useCallback, useRef, useState } from 'react';
import styled from 'styled-components';

const Container = styled.div<{ $dragActive?: boolean }>`
    padding: 16px;

    display: flex;
    flex-direction: column;
    align-items: center;

    border: 1px dashed ${(props) => (props.$dragActive ? colors.primary[500] : colors.gray[100])};
    border-radius: 12px;
`;

const InnerContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;

    gap: 8px;
`;

const IconContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;

    width: 32px;
    height: 32px;
    border-radius: 100%;
    background-color: ${colors.gray[1000]};
`;

const ActionTextContainer = styled.div`
    display: flex;
    gap: 4px;
`;

const InlineButton = styled(Button)`
    display: inline;
    padding: 0px;
    background: none;

    &:hover {
        background: none;
    }
`;

const Description = styled.div``;

interface Props {
    onFilesUpload?: (files: File[]) => Promise<void>;
    className?: string;
}

export function FileDragAndDropArea({ onFilesUpload, className }: Props) {
    const [dragActive, setDragActive] = useState<boolean>(false);

    const inputRef = useRef<HTMLInputElement>(null);

    const handleDrop = useCallback(
        async (e: React.DragEvent) => {
            e.preventDefault();
            e.stopPropagation();
            setDragActive(false);

            await onFilesUpload?.(Array.from(e.dataTransfer.files));
        },
        [onFilesUpload],
    );

    const onFileInputChange = useCallback(
        async (e: React.ChangeEvent<HTMLInputElement>) => {
            const { files } = e.target;
            if (files) await onFilesUpload?.(Array.from(files));
        },
        [onFilesUpload],
    );

    const onButtonClick = (e: React.MouseEvent) => {
        e.stopPropagation();
        e.preventDefault();
        inputRef.current?.click();
    };

    const handleDrag = useCallback((e: React.DragEvent) => {
        e.preventDefault();
        e.stopPropagation();
        if (e.type === 'dragenter' || e.type === 'dragover') {
            setDragActive(true);
        } else if (e.type === 'dragleave') {
            // Check if the next target is still inside our drop zone
            const dropZone = e.currentTarget;
            const related = e.relatedTarget;

            // If relatedTarget is null (e.g., leaving window) or outside drop zone
            if (!related || !dropZone.contains(related as Node)) {
                setDragActive(false);
            }
        }
    }, []);

    return (
        <>
            <Container
                onDrop={handleDrop}
                onDragEnter={handleDrag}
                onDragOver={handleDrag}
                onDragLeave={handleDrag}
                $dragActive={dragActive}
                className={className}
            >
                <InnerContainer>
                    <IconContainer onDragLeave={(e) => e.stopPropagation()}>
                        <Icon icon="UploadSimple" source="phosphor" color="primary" size="2xl" />
                    </IconContainer>
                    <ActionTextContainer>
                        <Text size="sm" weight="semiBold">
                            Drag a file or
                        </Text>{' '}
                        <InlineButton variant="text" size="sm" onClick={onButtonClick}>
                            click to upload
                        </InlineButton>
                    </ActionTextContainer>
                    <Description>
                        <Text size="sm" color="gray">
                            Max Size: 2GB
                        </Text>
                    </Description>
                </InnerContainer>
            </Container>

            <input ref={inputRef} type="file" multiple onChange={onFileInputChange} style={{ display: 'none' }} />
        </>
    );
}
