import { FileMarkdownOutlined, FileTextOutlined } from '@ant-design/icons';
import React, { useState } from 'react';
import ReactDiffViewer, { DiffMethod } from 'react-diff-viewer';
import styled from 'styled-components/macro';
import MDEditor from '@uiw/react-md-editor';
import { Avatar, Button, colors, Modal, Text, typography } from '@src/alchemy-components';
import { ActionRequest } from '@src/types.generated';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { toRelativeTimeString } from '@src/app/shared/time/timeUtils';
import { ANTD_GRAY } from '../../../entity/shared/constants';

const ButtonsWrapper = styled.div`
    display: flex;
    justify-content: flex-end;
    margin-bottom: 20px;
    gap: 16px;
`;

const HeadingContainer = styled.div`
    display: flex;
    justify-content: space-between;
    margin-top: 16px;
`;

const StyledButton = styled(Button)<{ $isSelected: boolean }>`
    ${(props) =>
        props.$isSelected &&
        `
        color: ${colors.violet[500]};
        &:focus {
            color: ${colors.violet[500]};
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
    min-width: 600px;

    pre {
        font-family: ${typography.fonts.body};
        font-size: 14px;
        color: ${colors.gray[1700]};
    }
`;

const DetailsContainer = styled.div`
    display: flex;
    gap: 8px;
`;

const TextContainer = styled.div`
    display: flex;
    flex-direction: column;
`;

const ViewerContainer = styled.div`
    overflow: auto;
`;

const commonStyles = {
    background: colors.gray[1500],
    fontSize: typography.fontSizes.md,
    fontFamily: typography.fonts.body,
};

const diffViewerStyles = {
    diffContainer: {
        ...commonStyles,
    },

    contentText: {
        color: colors.gray[1700],
    },

    diffAdded: {
        background: colors.green[0],
        color: colors.green[1000],
        pre: {
            color: colors.green[1000],
        },
    },
    diffRemoved: {
        background: colors.red[0],
        color: colors.red[1000],
        pre: {
            color: colors.red[1000],
        },
    },
    wordAdded: {
        background: colors.green[1300],
        color: colors.green[1000],
    },
    wordRemoved: {
        background: colors.red[100],
        color: colors.red[1000],
    },
};

interface Props {
    oldDescription: string;
    newDescription: string;
    closeModal: () => void;
    actionRequest: ActionRequest;
}

function DescriptionDifferenceModal({ oldDescription, newDescription, closeModal, actionRequest }: Props) {
    const entityRegistry = useEntityRegistryV2();
    const [isViewingMarkdown, setIsViewingMarkdown] = useState(true);
    const user = actionRequest.created.actor;
    const name = user ? entityRegistry.getDisplayName(user.type, user) : undefined;
    const entityName =
        actionRequest.entity?.type && entityRegistry.getDisplayName(actionRequest.entity?.type, actionRequest.entity);
    return (
        <StyledModal width="minContent" title="Comparing changes" footer={null} visible onCancel={closeModal}>
            {name && (
                <DetailsContainer>
                    <Avatar name={name} size="xl" />
                    <TextContainer>
                        <Text color="gray">
                            Description update requested by{' '}
                            <Text type="span" weight="semiBold">
                                {name}
                            </Text>
                        </Text>
                        <Text weight="normal" color="gray" size="sm">
                            {toRelativeTimeString(actionRequest.created.time)}
                        </Text>
                    </TextContainer>
                </DetailsContainer>
            )}
            <HeadingContainer>
                <Text size="lg" color="gray" weight="bold">
                    {entityName} description
                </Text>
                <ButtonsWrapper>
                    <StyledButton
                        variant="text"
                        color="gray"
                        $isSelected={isViewingMarkdown}
                        onClick={() => setIsViewingMarkdown(true)}
                    >
                        <FileMarkdownOutlined />
                        Markdown
                    </StyledButton>
                    <StyledButton
                        variant="text"
                        color="gray"
                        $isSelected={!isViewingMarkdown}
                        onClick={() => setIsViewingMarkdown(false)}
                    >
                        <FileTextOutlined />
                        Formatted Text
                    </StyledButton>
                </ButtonsWrapper>
            </HeadingContainer>
            <ViewerContainer>
                {isViewingMarkdown ? (
                    <ReactDiffViewer
                        oldValue={oldDescription}
                        newValue={newDescription}
                        compareMethod={DiffMethod.WORDS}
                        splitView
                        styles={diffViewerStyles}
                    />
                ) : (
                    <FormattedTextWrapper>
                        <MDEditor.Markdown
                            style={{ fontWeight: 400, flex: 1, color: colors.gray[500] }}
                            source={oldDescription}
                        />
                        <VerticalDivider />
                        <MDEditor.Markdown
                            style={{ fontWeight: 400, flex: 1, color: colors.gray[500] }}
                            source={newDescription}
                        />
                    </FormattedTextWrapper>
                )}
            </ViewerContainer>
        </StyledModal>
    );
}

export default DescriptionDifferenceModal;
