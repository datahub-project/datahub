import React, { useContext, useMemo, useState } from 'react';
import styled from 'styled-components/macro';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { Button, Modal } from 'antd';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import { useBaseEntity } from '../../../EntityContext';
import { QueryEntity } from '../../../../../../types.generated';
import EntitySidebarContext from '../../../../../shared/EntitySidebarContext';
import { SidebarSection } from './SidebarSection';

const PreviewSyntax = styled(SyntaxHighlighter)`
    max-width: 300px;
    max-height: 150px;
    overflow: hidden;
    mask-image: linear-gradient(to bottom, rgba(0, 0, 0, 1) 80%, rgba(255, 0, 0, 0.5) 85%, rgba(255, 0, 0, 0) 90%);

    span {
        font-family: 'Roboto Mono', monospace;
    }
`;

const ModalSyntaxContainer = styled.div`
    margin: 20px;
`;

export function SidebarDatasetViewDefinitionSection() {
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const statement = baseEntity?.dataset?.viewProperties?.logic;

    if (!statement) return null;

    return <SidebarLogicSection title="View Definition" statement={statement} />;
}

export function SidebarQueryLogicSection() {
    const baseEntity = useBaseEntity<{ entity: QueryEntity }>();
    const statement = baseEntity?.entity?.properties?.statement?.value;
    const { extra } = useContext(EntitySidebarContext);
    const stringToHighlight = extra?.transformOperation;

    if (!statement) return null;

    return <SidebarLogicSection title="Logic" statement={statement} stringToHighlight={stringToHighlight} />;
}

interface HelperProps {
    title: string;
    statement: string;
    stringToHighlight?: string;
}

function SidebarLogicSection({ title, statement, stringToHighlight }: HelperProps) {
    const [showFullContentModal, setShowFullContentModal] = useState(false);

    const lineNumberToHighlight = useMemo(() => {
        return findLineNumberToHighlight(statement, stringToHighlight || '');
    }, [statement, stringToHighlight]);

    return (
        <SidebarSection
            title={title}
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
                                    }
                                    return { style };
                                }}
                            >
                                {statement}
                            </SyntaxHighlighter>
                        </ModalSyntaxContainer>
                    </Modal>
                    <PreviewSyntax language="sql" wrapLongLines>
                        {statement}
                    </PreviewSyntax>
                    <Button type="text" onClick={() => setShowFullContentModal(true)}>
                        See Full
                    </Button>
                </>
            }
        />
    );
}

/** Find the line number of a target substring in a given string */
function findLineNumberToHighlight(inputString: string, targetSubstring: string) {
    if (!targetSubstring) {
        return -1;
    }

    const lines = inputString.split('\n');

    for (let lineNumber = 0; lineNumber < lines.length; lineNumber++) {
        if (lines[lineNumber].includes(targetSubstring)) {
            // Adding 1 because line numbers are 1-based, not 0-based
            return lineNumber + 1;
        }
    }

    // Return -1 if the target substring is not found in any line
    return -1;
}
