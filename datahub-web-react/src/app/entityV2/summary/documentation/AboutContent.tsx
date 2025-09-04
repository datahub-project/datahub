import { Editor } from '@components';
import React from 'react';
import styled from 'styled-components';

import DescriptionActionsBar from '@app/entityV2/summary/documentation/DescriptionActionsBar';
import DescriptionViewer from '@app/entityV2/summary/documentation/DescriptionViewer';
import EmptyDescription from '@app/entityV2/summary/documentation/EmptyDescription';
import { useDescriptionUtils } from '@app/entityV2/summary/documentation/useDescriptionUtils';
import { useDocumentationPermission } from '@app/entityV2/summary/documentation/useDocumentationPermission';

const StyledEditor = styled(Editor)<{ $isEditing?: boolean }>`
    border: none;
    margin-top: 4px;
    &&& {
        .remirror-editor {
            padding: 0;
        }
    }
    ${({ $isEditing }) =>
        $isEditing &&
        `
            &&& {
                    .remirror-editor-wrapper {
                        margin-top: 16px;
                    }
                }
    `}
`;

const DescriptionContainer = styled.div`
    width: max-content;
    max-width: 100%;
`;

const toolbarStyles = {
    marginLeft: '-8px',
};

export default function AboutContent() {
    const {
        isEditing,
        setIsEditing,
        initialDescription,
        updatedDescription,
        setUpdatedDescription,
        handleDescriptionUpdate,
        handleCancel,
        emptyDescriptionText,
    } = useDescriptionUtils();
    const canEditDescription = useDocumentationPermission();

    let content;

    if (!updatedDescription && !isEditing) {
        content = <EmptyDescription />;
    } else if (isEditing) {
        content = (
            <StyledEditor
                content={updatedDescription}
                placeholder={emptyDescriptionText}
                hideHighlightToolbar
                onChange={(description) => setUpdatedDescription(description)}
                $isEditing={isEditing}
                toolbarStyles={toolbarStyles}
            />
        );
    } else {
        content = (
            <DescriptionViewer>
                <StyledEditor content={updatedDescription} readOnly />
            </DescriptionViewer>
        );
    }
    return (
        <>
            <DescriptionContainer
                onClick={() => {
                    if (canEditDescription) {
                        setIsEditing(true);
                    }
                }}
            >
                {content}
            </DescriptionContainer>
            {isEditing && (
                <DescriptionActionsBar
                    onCancel={handleCancel}
                    onUpdate={handleDescriptionUpdate}
                    areActionsDisabled={updatedDescription === initialDescription}
                />
            )}
        </>
    );
}
