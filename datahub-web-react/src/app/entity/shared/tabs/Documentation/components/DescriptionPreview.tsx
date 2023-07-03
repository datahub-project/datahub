import React from 'react';
import styled from 'styled-components';
import { Editor } from './editor/Editor';
import { DescriptionPreviewToolbar } from './DescriptionPreviewToolbar';

const EditorContainer = styled.div`
    overflow: auto;
    height: 100%;
`;

type DescriptionPreviewProps = {
    description: string;
    onEdit: () => void;
};

export const DescriptionPreview = ({ description, onEdit }: DescriptionPreviewProps) => {
    return (
        <>
            <DescriptionPreviewToolbar onEdit={onEdit} />
            <EditorContainer>
                <Editor content={description} readOnly />
            </EditorContainer>
        </>
    );
};
