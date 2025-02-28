import React from 'react';
import styled from 'styled-components';
import InferenceDetailsPill from '@src/app/sharedV2/inferred/InferenceDetailsPill';
import { Editor } from './editor/Editor';
import { DescriptionPreviewToolbar } from './DescriptionPreviewToolbar';

const EditorContainer = styled.div`
    overflow: auto;
    height: 100%;
`;

type DescriptionPreviewProps = {
    description: string;
    isInferred?: boolean;
    onEdit: () => void;
};

export const DescriptionPreview = ({ description, isInferred, onEdit }: DescriptionPreviewProps) => {
    return (
        <>
            <DescriptionPreviewToolbar onEdit={onEdit} />
            <EditorContainer>
                {isInferred && <InferenceDetailsPill pillStyles={{ marginTop: 16, marginLeft: 24 }} />}
                <Editor content={description} readOnly />
            </EditorContainer>
        </>
    );
};
