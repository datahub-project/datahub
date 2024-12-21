import CopyQuery from '@src/app/entity/shared/tabs/Dataset/Queries/CopyQuery';
import { useIsEmbeddedProfile } from '@src/app/shared/useEmbeddedProfileLinkProps';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { GetDataJobQuery } from '@src/graphql/dataJob.generated';
import { Button, Modal } from 'antd';
import React, { useContext, useMemo, useState } from 'react';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import styled from 'styled-components/macro';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import { EntityType, QueryEntity } from '../../../../../../types.generated';
import { useBaseEntity } from '../../../../../entity/shared/EntityContext';
import { DBT_URN } from '../../../../../ingest/source/builder/constants';
import EntitySidebarContext from '../../../../../sharedV2/EntitySidebarContext';
import { ViewTab } from '../../../tabs/Dataset/View/ViewDefinitionTab';
import { SidebarSection } from './SidebarSection';

const PreviewSyntax = styled(SyntaxHighlighter)`
    max-width: 100%;
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

export const ViewHeader = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-top: 10px;
`;

export function SidebarDatasetViewDefinitionSection() {
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const statement = baseEntity?.dataset?.viewProperties?.logic;
    const entityRegistry = useEntityRegistry();
    const externalUrl = entityRegistry.getEntityUrl(EntityType.Dataset, baseEntity?.dataset?.urn || '');
    if (!statement) return null;

    return <SidebarLogicSection title="View Definition" statement={statement} externalUrl={externalUrl} />;
}

export function SidebarDataJobTransformationLogicSection() {
    const baseEntity = useBaseEntity<GetDataJobQuery>();
    const statement = baseEntity?.dataJob?.dataTransformLogic?.transforms?.[0]?.queryStatement?.value;
    const entityRegistry = useEntityRegistry();
    const externalUrl = entityRegistry.getEntityUrl(EntityType.DataJob, baseEntity?.dataJob?.urn || '');
    if (!statement) return null;

    return <SidebarLogicSection title="Logic" statement={statement} externalUrl={externalUrl} />;
}

export function SidebarQueryLogicSection() {
    const baseEntity = useBaseEntity<{ entity: QueryEntity }>();
    const statement = baseEntity?.entity?.properties?.statement?.value;
    const entityRegistry = useEntityRegistry();
    const externalUrl = entityRegistry.getEntityUrl(EntityType.Query, baseEntity?.entity?.urn || '');
    const { fineGrainedOperations } = useContext(EntitySidebarContext);
    const highlightedStrings = useMemo(
        () => fineGrainedOperations?.map((e) => e.transformOperation)?.filter((s): s is string => !!s),
        [fineGrainedOperations],
    );

    if (!statement) return null;

    return (
        <SidebarLogicSection
            title="Logic"
            statement={statement}
            highlightedStrings={highlightedStrings}
            externalUrl={externalUrl}
        />
    );
}

interface HelperProps {
    title: string;
    statement: string;
    highlightedStrings?: string[];
    externalUrl: string;
}

// exported for testing only
export function SidebarLogicSection({ title, statement, highlightedStrings, externalUrl }: HelperProps) {
    const [showFullContentModal, setShowFullContentModal] = useState(false);
    const isEmbeddedProfile = useIsEmbeddedProfile();

    const highlightedLineNumbers = new Set(highlightedStrings?.map((s) => findLineNumberToHighlight(statement, s)));

    function lineProps(lineNumber: number): React.HTMLProps<HTMLElement> {
        const style: React.CSSProperties = { display: 'block', width: 'fit-content' };
        if (highlightedLineNumbers.has(lineNumber)) {
            style.backgroundColor = 'rgba(134, 169, 244, 0.41)';
        }
        return { style };
    }
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const formattedLogic = baseEntity?.dataset?.viewProperties?.formattedLogic;

    const canShowFormatted = !!formattedLogic;

    const isDbt = baseEntity?.dataset?.platform?.urn === DBT_URN;
    const formatOptions = isDbt ? ['Source', 'Compiled'] : ['Raw', 'Formatted'];
    const [showFormatted, setShowFormatted] = useState(false);

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
                            <ViewHeader>
                                {canShowFormatted && (
                                    <ViewTab
                                        formatOptions={formatOptions}
                                        setShowFormatted={setShowFormatted}
                                        showFormatted={showFormatted}
                                    />
                                )}
                                <CopyQuery query={showFormatted ? formattedLogic || '' : statement} showCopyText />
                            </ViewHeader>
                            <SyntaxHighlighter language="sql" showLineNumbers lineProps={lineProps}>
                                {showFormatted ? formattedLogic : statement}
                            </SyntaxHighlighter>
                        </ModalSyntaxContainer>
                    </Modal>
                    {canShowFormatted && (
                        <ViewTab
                            formatOptions={formatOptions}
                            setShowFormatted={setShowFormatted}
                            showFormatted={showFormatted}
                        />
                    )}
                    <PreviewSyntax
                        language="sql"
                        showLineNumbers
                        wrapLines
                        lineNumberStyle={{ display: 'none' }}
                        lineProps={lineProps}
                    >
                        {showFormatted ? formattedLogic : statement}
                    </PreviewSyntax>
                    <Button
                        type="text"
                        onClick={() => {
                            if (isEmbeddedProfile) {
                                window.open(`${externalUrl}/View Definition`, '_blank');
                            } else {
                                setShowFullContentModal(true);
                            }
                        }}
                    >
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
