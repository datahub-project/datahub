import { message, Modal, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { useGetIngestionExecutionRequestQuery } from '../../../graphql/ingestion.generated';
import { Message } from '../../shared/Message';

const Section = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 12px;
`;

const SectionHeader = styled(Typography.Text)`
    &&&& {
        padding: 0px;
        margin: 0px;
        margin-bottom: 12px;
    }
`;

type Props = {
    urn: string;
    visible: boolean;
    onClose: () => void;
};

export const ExecutionDetailsModal = ({ urn, visible, onClose }: Props) => {
    const { data, loading, error } = useGetIngestionExecutionRequestQuery({ variables: { urn } });
    const output = data?.executionRequest?.result?.report || 'No output found.';

    return (
        <Modal
            width={1300}
            footer={null}
            title={<Typography.Text>Execution Details</Typography.Text>}
            visible={visible}
            onCancel={onClose}
        >
            {!data && loading && <Message type="loading" content="Loading execution details..." />}
            {error && message.error('Failed to load execution details :(')}
            <Section>
                <SectionHeader strong>Captured Output</SectionHeader>
                <Typography.Paragraph ellipsis>
                    <pre>{`${output}`}</pre>
                </Typography.Paragraph>
            </Section>
        </Modal>
    );
};
