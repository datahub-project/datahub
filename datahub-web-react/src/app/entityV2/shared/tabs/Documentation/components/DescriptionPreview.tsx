import React from 'react';
import styled from 'styled-components';

import { DescriptionPreviewToolbar } from '@app/entityV2/shared/tabs/Documentation/components/DescriptionPreviewToolbar';
import { Editor } from '@app/entityV2/shared/tabs/Documentation/components/editor/Editor';

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
