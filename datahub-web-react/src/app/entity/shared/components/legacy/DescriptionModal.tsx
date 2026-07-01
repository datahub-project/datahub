import { Button, Form, Modal, Typography } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { Editor } from '@app/entity/shared/tabs/Documentation/components/editor/Editor';

const FormLabel = styled(Typography.Text)`
    font-size: 10px;
    font-weight: bold;
`;

const StyledEditor = styled(Editor)`
    border: 1px solid ${(props) => props.theme.colors.border};
`;

const StyledViewer = styled(Editor)`
    .remirror-editor.ProseMirror {
        padding: 0;
    }
`;

const OriginalDocumentation = styled(Form.Item)`
    margin-bottom: 0;
`;

type Props = {
    title: string;
    description?: string | undefined;
    original?: string | undefined;
    propagatedDescription?: string | undefined;
    onClose: () => void;
    onSubmit: (description: string) => void;
    isAddDesc?: boolean;
};

export default function UpdateDescriptionModal({
    title,
    description,
    original,
    propagatedDescription,
    onClose,
    onSubmit,
    isAddDesc,
}: Props) {
    const { t } = useTranslation('entityV1.shared.components');
    const { t: tc } = useTranslation('common.actions');
    const [updatedDesc, setDesc] = useState(description || original || '');

    const handleEditorKeyDown = (event: React.KeyboardEvent<HTMLDivElement>) => {
        if (
            event.key === 'ArrowDown' ||
            event.key === 'ArrowUp' ||
            event.key === 'ArrowRight' ||
            event.key === 'ArrowLeft'
        ) {
            event.stopPropagation();
        }
    };

    return (
        <Modal
            title={title}
            open
            width={900}
            onCancel={onClose}
            okText={isAddDesc ? tc('submit') : tc('update')}
            footer={
                <>
                    <Button onClick={onClose}>{tc('cancel')}</Button>
                    <Button
                        onClick={() => onSubmit(updatedDesc)}
                        disabled={updatedDesc === description}
                        data-testid="description-modal-update-button"
                    >
                        {tc('update')}
                    </Button>
                </>
            }
        >
            <Form layout="vertical">
                <Form.Item>
                    <StyledEditor
                        content={updatedDesc}
                        onChange={setDesc}
                        dataTestId="description-editor"
                        onKeyDown={handleEditorKeyDown}
                    />
                </Form.Item>
                {!isAddDesc && description && original && (
                    <OriginalDocumentation label={<FormLabel>{t('descriptionModal.original')}</FormLabel>}>
                        <StyledViewer content={original || ''} readOnly />
                    </OriginalDocumentation>
                )}
                {!isAddDesc && description && propagatedDescription && (
                    <OriginalDocumentation label={<FormLabel>{t('descriptionModal.propagated')}</FormLabel>}>
                        <StyledViewer content={propagatedDescription || ''} readOnly />
                    </OriginalDocumentation>
                )}
            </Form>
        </Modal>
    );
}
