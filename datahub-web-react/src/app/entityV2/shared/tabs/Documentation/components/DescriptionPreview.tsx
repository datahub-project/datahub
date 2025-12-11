/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Editor } from '@components';
import React from 'react';
import styled from 'styled-components';

import { DescriptionPreviewToolbar } from '@app/entityV2/shared/tabs/Documentation/components/DescriptionPreviewToolbar';

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
