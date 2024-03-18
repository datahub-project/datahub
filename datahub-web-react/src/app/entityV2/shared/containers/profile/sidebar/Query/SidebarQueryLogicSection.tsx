import React, { useContext, useMemo } from 'react';
import styled from 'styled-components/macro';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { Button, Modal } from 'antd';

import { useBaseEntity } from '../../../../EntityContext';
import { SidebarSection } from '../SidebarSection';
import { QueryEntity } from '../../../../../../../types.generated';
import EntitySidebarContext from '../../../../../../shared/EntitySidebarContext';

/**
 * NOTE: To ensure consistent font-family for pre and code tags within as the parent wrapper was overriding it,
 * we explicitly apply 'Roboto Mono', monospace as the font-family for code children using span.
 */
const PreviewSyntax = styled(SyntaxHighlighter)`
    max-width: 300px !important;
    max-height: 150px !important;
    overflow: hidden !important;
    mask-image: linear-gradient(to bottom, rgba(0, 0, 0, 1) 80%, rgba(255, 0, 0, 0.5) 85%, rgba(255, 0, 0, 0) 90%);
    span {
        font-family: 'Roboto Mono', monospace !important;
    }
`;

const ModalSyntaxContainer = styled.div`
    margin: 20px;
`;

// Function to find the line number of a target substring in a given string
// This is a temporary implementation and will be removed once line number is included in operation(extra)
const findLineNumberToHighlight = (inputString: string, targetSubstring: string): number => {
    if (!targetSubstring) {
        return -1;
    }

    const lines: string[] = inputString.split('\n');

    for (let lineNumber = 0; lineNumber < lines.length; lineNumber++) {
        if (lines[lineNumber].includes(targetSubstring)) {
            // Adding 1 because line numbers are 1-based, not 0-based
            return lineNumber + 1;
        }
    }

    // Return -1 if the target substring is not found in any line
    return -1;
};

export default function SidebarQueryLogicSection() {
    const baseEntity = useBaseEntity<{ entity: QueryEntity }>();
    const [showFullContentModal, setShowFullContentModal] = React.useState(false);

    const { extra } = useContext(EntitySidebarContext);
    const stringToHighlight = extra?.transformOperation;

    const lineNumberToHighlight = useMemo(() => {
        return findLineNumberToHighlight(
            baseEntity?.entity?.properties?.statement?.value || '',
            stringToHighlight || '',
        );
    }, [baseEntity?.entity?.properties?.statement?.value, stringToHighlight]);

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
                            <SyntaxHighlighter
                                language="sql"
                                wrapLongLines
                                showLineNumbers
                                lineProps={(lineNumber: number): React.HTMLProps<HTMLElement> => {
                                    const style: React.CSSProperties = { display: 'block', width: 'fit-content' };
                                    if (lineNumberToHighlight === lineNumber) {
                                        style.backgroundColor = 'rgba(134, 169, 244, 0.41)';
                                        style.fontWeight = 'bold';
                                    }
                                    return { style };
                                }}
                            >
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
