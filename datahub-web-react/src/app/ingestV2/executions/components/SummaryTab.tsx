import { DownloadOutlined } from '@ant-design/icons';
import { Button, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import YAML from 'yamljs';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import { StructuredReport, hasSomethingToShow } from '@app/ingestV2/executions/components/reporting/StructuredReport';
import {
    EXECUTION_REQUEST_STATUS_SUCCEEDED_WITH_WARNINGS,
    EXECUTION_REQUEST_STATUS_SUCCESS,
} from '@app/ingestV2/executions/constants';
import { TabType } from '@app/ingestV2/executions/types';
import { getExecutionRequestSummaryText } from '@app/ingestV2/executions/utils';
import IngestedAssets from '@app/ingestV2/source/IngestedAssets';
import { getStructuredReport } from '@app/ingestV2/source/utils';
import { downloadFile } from '@app/search/utils/csvUtils';

import { GetIngestionExecutionRequestQuery } from '@graphql/ingestion.generated';
import { ExecutionRequestResult } from '@types';

const Section = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 12px;
`;

const SectionHeader = styled(Typography.Title)`
    &&&& {
        padding: 0px;
        margin: 0px;
        margin-bottom: 12px;
    }
`;

const SectionSubHeader = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

const ButtonGroup = styled.div`
    display: flex;
    gap: 8px;
    align-items: center;
`;

const SubHeaderParagraph = styled(Typography.Paragraph)`
    margin-bottom: 0px;
`;

const StatusSection = styled.div`
    border-bottom: 1px solid ${ANTD_GRAY[4]};
    padding: 16px;
    padding-left: 30px;
    padding-right: 30px;
`;

const IngestedAssetsSection = styled.div<{ isFirstSection?: boolean }>`
    border-bottom: 1px solid ${ANTD_GRAY[4]};
    ${({ isFirstSection }) => !isFirstSection && `border-top: 1px solid ${ANTD_GRAY[4]};`}
    padding: 16px;
    padding-left: 30px;
    padding-right: 30px;
`;

const RecipeSection = styled.div`
    border-top: 1px solid ${ANTD_GRAY[4]};
    padding-top: 16px;
    padding-left: 30px;
    padding-right: 30px;
`;

const LogsSection = styled.div`
    padding-top: 16px;
    padding-left: 30px;
    padding-right: 30px;
`;

const ShowMoreButton = styled(Button)`
    padding: 0px;
`;

const DetailsContainer = styled.div<DetailsContainerProps>`
    margin-bottom: -25px;
    ${(props) =>
        props.areDetailsExpandable &&
        !props.showExpandedDetails &&
        `
        -webkit-mask-image: linear-gradient(to bottom, rgba(0,0,0,1) 50%, rgba(255,0,0,0.5) 60%, rgba(255,0,0,0) 90% );
        mask-image: linear-gradient(to bottom, rgba(0,0,0,1) 50%, rgba(255,0,0,0.5) 60%, rgba(255,0,0,0) 90%);
    `}
`;

type DetailsContainerProps = {
    showExpandedDetails: boolean;
    areDetailsExpandable: boolean;
};

export const SummaryTab = ({
    urn,
    status,
    result,
    data,
    onTabChange,
}: {
    urn: string;
    status: string | undefined;
    result: Partial<ExecutionRequestResult>;
    data: GetIngestionExecutionRequestQuery | undefined;
    onTabChange: (tab: TabType) => void;
}) => {
    const [showExpandedLogs] = useState(false);
    const [showExpandedRecipe] = useState(false);

    const output = data?.executionRequest?.result?.report || 'No output found.';

    const downloadLogs = () => {
        downloadFile(output, `exec-${urn}.log`);
    };

    const logs = (showExpandedLogs && output) || output?.split('\n')?.slice(0, 14)?.join('\n');
    const structuredReport = result && getStructuredReport(result);
    const resultSummaryText =
        (status && status !== EXECUTION_REQUEST_STATUS_SUCCESS && (
            <Typography.Text type="secondary">{getExecutionRequestSummaryText(status)}</Typography.Text>
        )) ||
        undefined;
    const recipeJson = data?.executionRequest?.input?.arguments?.find((arg) => arg.key === 'recipe')?.value;
    let recipeYaml: string;
    try {
        recipeYaml = recipeJson && YAML.stringify(JSON.parse(recipeJson), 8, 2).trim();
    } catch (e) {
        recipeYaml = '';
    }
    const recipe = showExpandedRecipe ? recipeYaml : recipeYaml?.split('\n')?.slice(0, 14)?.join('\n');
    const areLogsExpandable = output?.split(/\r\n|\r|\n/)?.length > 14;
    const isRecipeExpandable = recipeYaml?.split(/\r\n|\r|\n/)?.length > 14;

    const downloadRecipe = () => {
        downloadFile(recipeYaml, `recipe-${urn}.yaml`);
    };

    return (
        <Section>
            {(resultSummaryText || (structuredReport && hasSomethingToShow(structuredReport))) && (
                <StatusSection>
                    <SubHeaderParagraph>{resultSummaryText}</SubHeaderParagraph>
                    {structuredReport && structuredReport ? <StructuredReport report={structuredReport} /> : null}
                </StatusSection>
            )}
            {(status === EXECUTION_REQUEST_STATUS_SUCCESS ||
                status === EXECUTION_REQUEST_STATUS_SUCCEEDED_WITH_WARNINGS) && (
                <IngestedAssetsSection
                    isFirstSection={!resultSummaryText && !(structuredReport && hasSomethingToShow(structuredReport))}
                >
                    {data?.executionRequest?.id && (
                        <IngestedAssets executionResult={result} id={data?.executionRequest?.id} />
                    )}
                </IngestedAssetsSection>
            )}
            <LogsSection>
                <SectionHeader level={5}>Logs</SectionHeader>
                <SectionSubHeader>
                    <SubHeaderParagraph type="secondary">
                        View logs that were collected during the sync.
                    </SubHeaderParagraph>
                    <ButtonGroup>
                        {areLogsExpandable && (
                            <ShowMoreButton type="text" onClick={() => onTabChange(TabType.Logs)}>
                                View More
                            </ShowMoreButton>
                        )}
                        <Button type="text" onClick={downloadLogs}>
                            <DownloadOutlined />
                        </Button>
                    </ButtonGroup>
                </SectionSubHeader>
                <DetailsContainer areDetailsExpandable={areLogsExpandable} showExpandedDetails={false}>
                    <Typography.Paragraph ellipsis>
                        <pre>{`${logs}${areLogsExpandable ? '...' : ''}`}</pre>
                    </Typography.Paragraph>
                </DetailsContainer>
            </LogsSection>
            {recipe && (
                <RecipeSection>
                    <SectionHeader level={5}>Recipe</SectionHeader>
                    <SectionSubHeader>
                        <SubHeaderParagraph type="secondary">
                            The configurations used for this sync with the data source.
                        </SubHeaderParagraph>
                        <ButtonGroup>
                            {isRecipeExpandable && (
                                <ShowMoreButton type="text" onClick={() => onTabChange(TabType.Recipe)}>
                                    View More
                                </ShowMoreButton>
                            )}
                            <Button type="text" onClick={downloadRecipe}>
                                <DownloadOutlined />
                            </Button>
                        </ButtonGroup>
                    </SectionSubHeader>
                    <DetailsContainer
                        areDetailsExpandable={isRecipeExpandable}
                        showExpandedDetails={showExpandedRecipe}
                    >
                        <Typography.Paragraph ellipsis>
                            <pre>{`${recipe}${!showExpandedRecipe && isRecipeExpandable ? '...' : ''}`}</pre>
                        </Typography.Paragraph>
                    </DetailsContainer>
                </RecipeSection>
            )}
        </Section>
    );
};
