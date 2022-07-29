import { FileMarkdownOutlined, FileTextOutlined } from '@ant-design/icons';
import { blue } from '@ant-design/colors';
import { Button, Modal } from 'antd';
import React, { useState } from 'react';
import ReactDiffViewer, { DiffMethod } from 'react-diff-viewer';
import styled from 'styled-components/macro';
import MDEditor from '@uiw/react-md-editor';
import { ANTD_GRAY } from '../../../entity/shared/constants';

const ButtonsWrapper = styled.div`
    display: flex;
    justify-content: flex-end;
    margin-bottom: 20px;
`;

const StyledButton = styled(Button)<{ isSelected: boolean }>`
    ${(props) =>
        props.isSelected &&
        `
        color: ${blue[5]};
        &:focus {
            color: ${blue[5]};
        }    
    `}
`;

const FormattedTextWrapper = styled.div`
    display: flex;
`;

const VerticalDivider = styled.div`
    border: 0.5px solid ${ANTD_GRAY[6]};
    min-height: 100%;
    margin: 0 15px;
`;

const StyledModal = styled(Modal)`
    max-width: min(1300px, 97vw);
`;

interface Props {
    oldDescription: string;
    newDescription: string;
    closeModal: () => void;
}

function DescriptionDifferenceModal({ oldDescription, newDescription, closeModal }: Props) {
    const [isViewingMarkdown, setIsViewingMarkdown] = useState(true);

    return (
        <StyledModal width="minContent" title="Update Description Proposal" footer={null} visible onCancel={closeModal}>
            <ButtonsWrapper>
                <StyledButton type="text" isSelected={isViewingMarkdown} onClick={() => setIsViewingMarkdown(true)}>
                    <FileMarkdownOutlined />
                    Markdown
                </StyledButton>
                <StyledButton type="text" isSelected={!isViewingMarkdown} onClick={() => setIsViewingMarkdown(false)}>
                    <FileTextOutlined />
                    Formatted Text
                </StyledButton>
            </ButtonsWrapper>
            {isViewingMarkdown ? (
                <ReactDiffViewer
                    oldValue={oldDescription}
                    newValue={newDescription}
                    compareMethod={DiffMethod.WORDS}
                    splitView
                />
            ) : (
                <FormattedTextWrapper>
                    <MDEditor.Markdown style={{ fontWeight: 400, flex: 1 }} source={oldDescription} />
                    <VerticalDivider />
                    <MDEditor.Markdown style={{ fontWeight: 400, flex: 1 }} source={newDescription} />
                </FormattedTextWrapper>
            )}
        </StyledModal>
    );
}

export default DescriptionDifferenceModal;
