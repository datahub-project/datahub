import { Button, CodeBlock, Modal } from '@components';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

const PreviewCode = styled(CodeBlock)`
    max-width: 100%;
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

const PREVIEW_MAX_HEIGHT = 68;

const SQL_LANGUAGE = 'sql';

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
                    <CodeBlock
                        code={query}
                        language={SQL_LANGUAGE}
                        showHeader={false}
                        showCopy={false}
                        showFormat={false}
                        showLineNumbers
                        wrap
                    />
                </ModalSyntaxContainer>
            </Modal>

            <PreviewCode
                code={query}
                language={SQL_LANGUAGE}
                showHeader={false}
                showCopy={false}
                showFormat={false}
                wrap
                maxHeight={PREVIEW_MAX_HEIGHT}
                overflow="hidden"
            />
            <Button variant="text" onClick={() => setShowFullContentModal(true)}>
                {tc('readMore')}
            </Button>
        </Container>
    );
};

export default SummaryQuerySection;
