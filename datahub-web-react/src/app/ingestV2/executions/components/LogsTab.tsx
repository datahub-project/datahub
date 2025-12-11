/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { DownloadOutlined } from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components';

import { DetailsContainer, SectionHeader } from '@app/ingestV2/executions/components/BaseTab';
import { downloadFile } from '@app/search/utils/csvUtils';
import { Button, Text, Tooltip } from '@src/alchemy-components';

import { GetIngestionExecutionRequestQuery } from '@graphql/ingestion.generated';

const SectionSubHeader = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

const SubHeaderParagraph = styled(Text)`
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
                <SubHeaderParagraph color="gray" colorLevel={600}>
                    View logs that were collected during the sync.
                </SubHeaderParagraph>
                <Tooltip title="Download Logs">
                    <Button variant="text" onClick={downloadLogs}>
                        <DownloadOutlined />
                    </Button>
                </Tooltip>
            </SectionSubHeader>
            <DetailsContainer>
                <Text size="sm">
                    <pre>{output}</pre>
                </Text>
            </DetailsContainer>
        </LogsSection>
    );
};
