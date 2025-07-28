import { DownloadOutlined } from '@ant-design/icons';
import { Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import YAML from 'yamljs';

import { DetailsContainer, SectionBase, SectionHeader } from '@app/ingestV2/executions/components/BaseTab';
import { StructuredReport, hasSomethingToShow } from '@app/ingestV2/executions/components/reporting/StructuredReport';
import { EXECUTION_REQUEST_STATUS_SUCCESS } from '@app/ingestV2/executions/constants';
import { TabType } from '@app/ingestV2/executions/types';
import { getExecutionRequestSummaryText } from '@app/ingestV2/executions/utils';
import IngestedAssets from '@app/ingestV2/source/IngestedAssets';
import { getStructuredReport } from '@app/ingestV2/source/utils';
import { downloadFile } from '@app/search/utils/csvUtils';
import { Button, Text, Tooltip } from '@src/alchemy-components';

import { GetIngestionExecutionRequestQuery } from '@graphql/ingestion.generated';
import { ExecutionRequestResult } from '@types';

const Section = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 12px;
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

const SubHeaderParagraph = styled(Text)`
    margin-bottom: 0px;
`;

const StatusSection = styled.div`
    padding: 16px;
    padding-left: 30px;
    padding-right: 30px;
`;

const IngestedAssetsSection = styled.div`
    padding: 16px;
    padding-left: 30px;
    padding-right: 30px;
`;

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
    const [showExpandedLogs] = useState(true);
    const [showExpandedRecipe] = useState(true);

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
                    {!structuredReport && resultSummaryText && (
                        <SubHeaderParagraph>{resultSummaryText}</SubHeaderParagraph>
                    )}
                    {structuredReport && <StructuredReport report={structuredReport} />}
                </StatusSection>
            )}
            <IngestedAssetsSection>
                {data?.executionRequest?.id && (
                    <IngestedAssets executionResult={result} id={data?.executionRequest?.id} />
                )}
            </IngestedAssetsSection>
            <SectionBase>
                <SectionHeader level={5}>Logs</SectionHeader>
                <SectionSubHeader>
                    <SubHeaderParagraph color="gray" colorLevel={600}>
                        View logs that were collected during the sync.
                    </SubHeaderParagraph>
                    <ButtonGroup>
                        {areLogsExpandable && (
                            <Button variant="text" onClick={() => onTabChange(TabType.Logs)}>
                                View All
                            </Button>
                        )}
                        <Tooltip title="Download Logs">
                            <Button variant="text" onClick={downloadLogs}>
                                <DownloadOutlined />
                            </Button>
                        </Tooltip>
                    </ButtonGroup>
                </SectionSubHeader>
                <DetailsContainer>
                    <Text size="sm">
                        <pre>{logs}</pre>
                    </Text>
                </DetailsContainer>
            </SectionBase>
            {recipe && (
                <SectionBase>
                    <SectionHeader level={5}>Recipe</SectionHeader>
                    <SectionSubHeader>
                        <SubHeaderParagraph color="gray" colorLevel={600}>
                            The configurations used for this sync with the data source.
                        </SubHeaderParagraph>
                        <ButtonGroup>
                            {isRecipeExpandable && (
                                <Button variant="text" onClick={() => onTabChange(TabType.Recipe)}>
                                    View More
                                </Button>
                            )}
                            <Tooltip title="Download Recipe">
                                <Button variant="text" onClick={downloadRecipe}>
                                    <DownloadOutlined />
                                </Button>
                            </Tooltip>
                        </ButtonGroup>
                    </SectionSubHeader>
                    <DetailsContainer>
                        <Text size="sm">
                            <pre>{recipe}</pre>
                        </Text>
                    </DetailsContainer>
                </SectionBase>
            )}
        </Section>
    );
};
