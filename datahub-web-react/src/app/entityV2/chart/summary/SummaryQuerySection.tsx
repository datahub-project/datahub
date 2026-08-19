import { Modal } from '@components';
import { Button } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import styled from 'styled-components/macro';

const PreviewSyntax = styled(SyntaxHighlighter)`
    max-height: 68px;
    overflow: hidden !important;
    border-radius: 12px;
    max-width: 100%;
    background: ${(props) => props.theme.colors.bgSurface} !important;

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
    color: ${(props) => props.theme.colors.textBrand};
    display: flex;
    width: fit-content;

    :hover {
        color: ${(props) => props.theme.colors.buttonFillBrand};
        background: transparent;
    }
`;

interface Props {
    query: string;
}

const SummaryQuerySection = ({ query }: Props) => {
    const { t } = useTranslation('entity.types');
    const { t: tc } = useTranslation('common.actions');
    const [showFullContentModal, setShowFullContentModal] = useState(false);

    return (
        <Container>
            <Modal
                title={t('query.name')}
                width="800px"
                buttons={[
                    {
                        text: t('chart.dismiss'),
                        onClick: () => setShowFullContentModal(false),
                        variant: 'filled',
                    },
                ]}
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
                {tc('readMore')}
            </StyledButton>
        </Container>
    );
};

export default SummaryQuerySection;
