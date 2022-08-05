import { DownloadOutlined } from '@ant-design/icons';
import { Button, message, Modal, Typography } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { useGetIngestionExecutionRequestQuery } from '../../../graphql/ingestion.generated';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { downloadFile } from '../../search/utils/csvUtils';
import { Message } from '../../shared/Message';
import IngestedAssets from './IngestedAssets';
import {
    getExecutionRequestStatusDisplayColor,
    getExecutionRequestStatusDisplayText,
    getExecutionRequestStatusIcon,
} from './utils';

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

const HeaderSection = styled.div``;

const StatusSection = styled.div`
    border-bottom: 1px solid ${ANTD_GRAY[4]};
    padding: 16px;
    padding-left: 30px;
    padding-right: 30px;
`;

const IngestedAssetsSection = styled.div`
    border-bottom: 1px solid ${ANTD_GRAY[4]};
    padding: 16px;
    padding-left: 30px;
    padding-right: 30px;
`;

const LogsSection = styled.div`
    padding-top: 16px;
    padding-left: 30px;
    padding-right: 30px;
`;

type Props = {
    urn: string;
    visible: boolean;
    onClose: () => void;
};

export const ExecutionDetailsModal = ({ urn, visible, onClose }: Props) => {
    const [showExpandedLogs, setShowExpandedLogs] = useState(false);
    const { data, loading, error } = useGetIngestionExecutionRequestQuery({ variables: { urn } });
    const output = data?.executionRequest?.result?.report || 'No output found.';

    useEffect(() => {
        if (output.length > 100) {
            setShowExpandedLogs(false);
        }
    }, [output, setShowExpandedLogs]);

    const downloadLogs = () => {
        downloadFile(output, `exec-${urn}.log`);
    };

    const logs = (showExpandedLogs && output) || output.slice(0, 100);
    const result = data?.executionRequest?.result?.status;

    const ResultIcon = result && getExecutionRequestStatusIcon(result);
    const resultColor = result && getExecutionRequestStatusDisplayColor(result);
    const resultText = result && (
        <Typography.Text style={{ color: resultColor, fontSize: 14 }}>
            {ResultIcon && <ResultIcon style={{ marginRight: 4 }} />}
            {getExecutionRequestStatusDisplayText(result)}
        </Typography.Text>
    );
    const resultSummaryText =
        (result && (
            <Typography.Text type="secondary">
                Ingestion {result === 'SUCCESS' ? 'successfully completed' : 'completed with errors'}.
            </Typography.Text>
        )) ||
        undefined;

    return (
        <Modal
            width={800}
            footer={
                <>
                    <Button onClick={onClose}>Close</Button>
                </>
            }
            style={{ top: 100 }}
            bodyStyle={{ padding: 0 }}
            title={
                <HeaderSection>
                    <Typography.Title style={{ padding: 0, margin: 0 }} level={4}>
                        Ingestion Run Details
                    </Typography.Title>
                </HeaderSection>
            }
            visible={visible}
            onCancel={onClose}
        >
            {!data && loading && <Message type="loading" content="Loading execution details..." />}
            {error && message.error('Failed to load execution details :(')}
            <Section>
                <StatusSection>
                    <Typography.Title level={5}>Status</Typography.Title>
                    <div style={{ marginBottom: 4 }}>{resultText}</div>
                    <div>{resultSummaryText}</div>
                </StatusSection>
                {result === 'SUCCESS' && (
                    <IngestedAssetsSection>
                        {data?.executionRequest?.id && <IngestedAssets urn={urn} id={data?.executionRequest?.id} />}
                    </IngestedAssetsSection>
                )}
                <LogsSection>
                    <SectionHeader level={5}>Logs</SectionHeader>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                        <Typography.Paragraph style={{ marginBottom: 0 }} type="secondary">
                            View logs that were collected during the ingestion run.
                        </Typography.Paragraph>
                        <Button type="text" onClick={downloadLogs}>
                            <DownloadOutlined />
                            Download
                        </Button>
                    </div>
                    <Typography.Paragraph ellipsis>
                        <pre>{`${logs}...`}</pre>
                        {!showExpandedLogs && (
                            <Button style={{ padding: 0 }} type="link" onClick={() => setShowExpandedLogs(true)}>
                                Show More
                            </Button>
                        )}
                    </Typography.Paragraph>
                </LogsSection>
            </Section>
        </Modal>
    );
};
