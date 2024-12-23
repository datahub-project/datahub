import { DownloadOutlined } from '@ant-design/icons';
import { Button, message, Modal, Typography } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import YAML from 'yamljs';
import { useGetIngestionExecutionRequestQuery } from '../../../../graphql/ingestion.generated';
import { ANTD_GRAY } from '../../../entity/shared/constants';
import { downloadFile } from '../../../search/utils/csvUtils';
import { Message } from '../../../shared/Message';
import IngestedAssets from '../IngestedAssets';
import {
    getExecutionRequestStatusDisplayColor,
    getExecutionRequestStatusDisplayText,
    getExecutionRequestStatusIcon,
    getExecutionRequestSummaryText,
    getIngestionSourceStatus,
    getStructuredReport,
    RUNNING,
    SUCCESS,
    SUCCEEDED_WITH_WARNINGS,
} from '../utils';
import { ExecutionRequestResult } from '../../../../types.generated';
import { StructuredReport } from './reporting/StructuredReport';

const StyledTitle = styled(Typography.Title)`
    padding: 0px;
    margin: 0px;
`;

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

const SubHeaderParagraph = styled(Typography.Paragraph)`
    margin-bottom: 0px;
`;

const HeaderSection = styled.div``;

const StatusSection = styled.div`
    border-bottom: 1px solid ${ANTD_GRAY[4]};
    padding: 16px;
    padding-left: 30px;
    padding-right: 30px;
`;

const ResultText = styled.div`
    margin-bottom: 4px;
`;

const IngestedAssetsSection = styled.div`
    border-bottom: 1px solid ${ANTD_GRAY[4]};
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

const modalStyle = {
    top: 100,
};

const modalBodyStyle = {
    padding: 0,
};

type DetailsContainerProps = {
    showExpandedDetails: boolean;
    areDetailsExpandable: boolean;
};

type Props = {
    urn: string;
    open: boolean;
    onClose: () => void;
};

export const ExecutionDetailsModal = ({ urn, open, onClose }: Props) => {
    const [showExpandedLogs, setShowExpandedLogs] = useState(false);
    const [showExpandedRecipe, setShowExpandedRecipe] = useState(false);

    const { data, loading, error, refetch } = useGetIngestionExecutionRequestQuery({ variables: { urn } });
    const output = data?.executionRequest?.result?.report || 'No output found.';

    const downloadLogs = () => {
        downloadFile(output, `exec-${urn}.log`);
    };

    const logs = (showExpandedLogs && output) || output?.split('\n')?.slice(0, 5)?.join('\n');
    const result = data?.executionRequest?.result as Partial<ExecutionRequestResult>;
    const status = getIngestionSourceStatus(result);

    useEffect(() => {
        const interval = setInterval(() => {
            if (status === RUNNING) refetch();
        }, 2000);

        return () => clearInterval(interval);
    });

    const ResultIcon = status && getExecutionRequestStatusIcon(status);
    const resultColor = status && getExecutionRequestStatusDisplayColor(status);
    const resultText = status && (
        <Typography.Text style={{ color: resultColor, fontSize: 14 }}>
            {ResultIcon && <ResultIcon style={{ marginRight: 4 }} />}
            {getExecutionRequestStatusDisplayText(status)}
        </Typography.Text>
    );

    const structuredReport = result && getStructuredReport(result);

    const resultSummaryText =
        (status && <Typography.Text type="secondary">{getExecutionRequestSummaryText(status)}</Typography.Text>) ||
        undefined;

    const recipeJson = data?.executionRequest?.input?.arguments?.find((arg) => arg.key === 'recipe')?.value;
    let recipeYaml: string;
    try {
        recipeYaml = recipeJson && YAML.stringify(JSON.parse(recipeJson), 8, 2).trim();
    } catch (e) {
        recipeYaml = '';
    }
    const recipe = showExpandedRecipe ? recipeYaml : recipeYaml?.split('\n')?.slice(0, 5)?.join('\n');

    const areLogsExpandable = output?.split(/\r\n|\r|\n/)?.length > 5;
    const isRecipeExpandable = recipeYaml?.split(/\r\n|\r|\n/)?.length > 5;

    return (
        <Modal
            width={800}
            footer={<Button onClick={onClose}>Close</Button>}
            style={modalStyle}
            bodyStyle={modalBodyStyle}
            title={
                <HeaderSection>
                    <StyledTitle level={4}>Sync Details</StyledTitle>
                </HeaderSection>
            }
            open={open}
            onCancel={onClose}
        >
            {!data && loading && <Message type="loading" content="Loading sync details..." />}
            {error && message.error('Failed to load sync details :(')}
            <Section>
                <StatusSection>
                    <Typography.Title level={5}>Status</Typography.Title>
                    <ResultText>{resultText}</ResultText>
                    <SubHeaderParagraph>{resultSummaryText}</SubHeaderParagraph>
                    {structuredReport ? <StructuredReport report={structuredReport} /> : null}
                </StatusSection>
                {(status === SUCCESS || status === SUCCEEDED_WITH_WARNINGS) && (
                    <IngestedAssetsSection>
                        {data?.executionRequest?.id && <IngestedAssets id={data?.executionRequest?.id} />}
                    </IngestedAssetsSection>
                )}
                <LogsSection>
                    <SectionHeader level={5}>Logs</SectionHeader>
                    <SectionSubHeader>
                        <SubHeaderParagraph type="secondary">
                            View logs that were collected during the sync.
                        </SubHeaderParagraph>
                        <Button type="text" onClick={downloadLogs}>
                            <DownloadOutlined />
                            Download
                        </Button>
                    </SectionSubHeader>
                    <DetailsContainer areDetailsExpandable={areLogsExpandable} showExpandedDetails={showExpandedLogs}>
                        <Typography.Paragraph ellipsis>
                            <pre>{`${logs}${!showExpandedLogs && areLogsExpandable ? '...' : ''}`}</pre>
                        </Typography.Paragraph>
                    </DetailsContainer>
                    {areLogsExpandable && (
                        <ShowMoreButton type="link" onClick={() => setShowExpandedLogs(!showExpandedLogs)}>
                            {showExpandedLogs ? 'Hide' : 'Show More'}
                        </ShowMoreButton>
                    )}
                </LogsSection>
                {recipe && (
                    <RecipeSection>
                        <SectionHeader level={5}>Recipe</SectionHeader>
                        <SectionSubHeader>
                            <SubHeaderParagraph type="secondary">
                                The configurations used for this sync with the data source.
                            </SubHeaderParagraph>
                        </SectionSubHeader>
                        <DetailsContainer
                            areDetailsExpandable={isRecipeExpandable}
                            showExpandedDetails={showExpandedRecipe}
                        >
                            <Typography.Paragraph ellipsis>
                                <pre>{`${recipe}${!showExpandedRecipe && isRecipeExpandable ? '...' : ''}`}</pre>
                            </Typography.Paragraph>
                        </DetailsContainer>
                        {isRecipeExpandable && (
                            <ShowMoreButton type="link" onClick={() => setShowExpandedRecipe((v) => !v)}>
                                {showExpandedRecipe ? 'Hide' : 'Show More'}
                            </ShowMoreButton>
                        )}
                    </RecipeSection>
                )}
            </Section>
        </Modal>
    );
};
