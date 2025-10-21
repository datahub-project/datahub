import { Editor } from '@components';
import React from 'react';
import styled from 'styled-components';

import { DescriptionPreviewToolbar } from '@app/entityV2/shared/tabs/Documentation/components/DescriptionPreviewToolbar';
import InferenceDetailsPill from '@src/app/sharedV2/inferred/InferenceDetailsPill';

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
