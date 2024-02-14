import React from 'react';
import styled from 'styled-components/macro';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { Button, Modal } from 'antd';

import { useBaseEntity } from '../../../../EntityContext';
import { SidebarSection } from '../SidebarSection';
import { QueryEntity } from '../../../../../../../types.generated';

const PreviewSyntax = styled(SyntaxHighlighter)`
    margin-top: -10px !important;
    max-width: 300px !important;
    max-height: 150px !important;
    overflow: hidden !important;
    mask-image: linear-gradient(to bottom, rgba(0, 0, 0, 1) 80%, rgba(255, 0, 0, 0.5) 85%, rgba(255, 0, 0, 0) 90%);
`;

const ModalSyntaxContainer = styled.div`
    margin: 20px;
`;

export default function SidebarQueryLogicSection() {
    const baseEntity = useBaseEntity<{ entity: QueryEntity }>();
    const [showFullContentModal, setShowFullContentModal] = React.useState(false);

    return (
        <SidebarSection
            title="Logic"
            content={
                <>
                    <Modal
                        closeIcon={null}
                        width="1000px"
                        footer={
                            <Button key="back" onClick={() => setShowFullContentModal(false)}>
                                Dismiss
                            </Button>
                        }
                        open={showFullContentModal}
                        onCancel={() => setShowFullContentModal(false)}
                    >
                        <ModalSyntaxContainer>
                            <SyntaxHighlighter language="sql" wrapLongLines>
                                {baseEntity?.entity?.properties?.statement?.value || ''}
                            </SyntaxHighlighter>
                        </ModalSyntaxContainer>
                    </Modal>
                    <PreviewSyntax language="sql" wrapLongLines>
                        {baseEntity?.entity?.properties?.statement?.value || ''}
                    </PreviewSyntax>
                    <Button type="text" onClick={() => setShowFullContentModal(true)}>
                        See Full
                    </Button>
                </>
            }
        />
    );
}
