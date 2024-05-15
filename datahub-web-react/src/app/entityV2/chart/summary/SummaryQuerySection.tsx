import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { Button, Modal } from 'antd';
import { REDESIGN_COLORS } from '../../shared/constants';

const PreviewSyntax = styled(SyntaxHighlighter)`
    max-height: 68px;
    overflow: hidden !important;
    border-radius: 12px;
    max-width: 100%;
    background: #fafafc !important;

    span {
        font-family: 'Roboto Mono', monospace;
    }
`;

const ModalSyntaxContainer = styled.div`
    margin: 20px;
    overflow: auto;
`;

const Container = styled.div`
    display: flex;
    flex-direction: column;
    flex-wrap: wrap;
    max-width: 400px;
`;

const StyledButton = styled(Button)`
    color: ${REDESIGN_COLORS.TITLE_PURPLE};
    display: flex;
    width: fit-content;

    :hover {
        color: ${REDESIGN_COLORS.HOVER_PURPLE};
        background: transparent;
    }
`;

interface Props {
    query: string;
}

const SummaryQuerySection = ({ query }: Props) => {
    const [showFullContentModal, setShowFullContentModal] = useState(false);

    return (
        <Container>
            <Modal
                closeIcon={null}
                width="800px"
                footer={<Button onClick={() => setShowFullContentModal(false)}>Dismiss</Button>}
                open={showFullContentModal}
                onCancel={() => setShowFullContentModal(false)}
            >
                <ModalSyntaxContainer>
                    <SyntaxHighlighter language="sql" wrapLongLines showLineNumbers>
                        {query}
                    </SyntaxHighlighter>
                </ModalSyntaxContainer>
            </Modal>

            <PreviewSyntax language="sql" wrapLongLines>
                {query}
            </PreviewSyntax>
            <StyledButton type="text" onClick={() => setShowFullContentModal(true)}>
                Read More
            </StyledButton>
        </Container>
    );
};

export default SummaryQuerySection;
