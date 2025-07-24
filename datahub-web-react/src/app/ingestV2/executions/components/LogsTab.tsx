import { DownloadOutlined } from '@ant-design/icons';
import { Button, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { SectionHeader } from '@app/ingestV2/executions/components/BaseTab';
import { downloadFile } from '@app/search/utils/csvUtils';

import { GetIngestionExecutionRequestQuery } from '@graphql/ingestion.generated';

const SectionSubHeader = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

const SubHeaderParagraph = styled(Typography.Paragraph)`
    margin-bottom: 0px;
`;

const LogsSection = styled.div`
    padding-top: 16px;
    padding-left: 30px;
    padding-right: 30px;
`;

export const LogsTab = ({ urn, data }: { urn: string; data: GetIngestionExecutionRequestQuery | undefined }) => {
    const output = data?.executionRequest?.result?.report || 'No output found.';

    const downloadLogs = () => {
        downloadFile(output, `exec-${urn}.log`);
    };

    return (
        <LogsSection>
            <SectionHeader level={5}>Logs</SectionHeader>
            <SectionSubHeader>
                <SubHeaderParagraph type="secondary">View logs that were collected during the sync.</SubHeaderParagraph>
                <Button type="text" onClick={downloadLogs}>
                    <DownloadOutlined />
                    Download
                </Button>
            </SectionSubHeader>
            <Typography.Paragraph ellipsis>
                <pre>{output}</pre>
            </Typography.Paragraph>
        </LogsSection>
    );
};
