import React from 'react';
import styled from 'styled-components';
import { Button, List, message, Modal, Tag, Typography } from 'antd';
import { DeleteOutlined } from '@ant-design/icons';
import { IngestionSource } from '../../types.generated';
import { useDeleteIngestionSourceMutation } from '../../graphql/ingestion.generated';

const SourceItemContainer = styled.div`
    display: flex;
    justify-content: space-between;
    padding-left: 8px;
    padding-right: 8px;
    width: 100%;
`;

const SourceHeaderContainer = styled.div`
    display: flex;
    justify-content: left;
    align-items: center;
`;

type Props = {
    source: IngestionSource;
    onClick?: () => void;
    onExecute?: () => void;
    onDelete?: () => void;
};

export default function IngestionSourceListItem({ source, onClick, onExecute, onDelete }: Props) {
    const [removeIngestionSourceMutation] = useDeleteIngestionSourceMutation();

    const onRemoveIngestionSource = async (urn: string) => {
        try {
            await removeIngestionSourceMutation({
                variables: { urn },
            });
            message.success({ content: 'Removed ingestion source.', duration: 2 });
            onDelete?.();
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error({ content: `Failed to remove ingestion source: \n ${e.message || ''}`, duration: 3 });
            }
        }
    };

    const handleRemoveIngestionSource = (urn: string) => {
        Modal.confirm({
            title: `Confirm Ingestion Source Removal`,
            content: `Are you sure you want to remove this ingestion source?`,
            onOk() {
                onRemoveIngestionSource(urn);
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    const { name, schedule } = source;
    const cron = schedule?.interval;
    const totalExecutions = source.executions?.total;

    return (
        <>
            <List.Item>
                <SourceItemContainer>
                    <SourceHeaderContainer>
                        <Button type="text" onClick={onClick}>
                            <div style={{ marginLeft: 16, marginRight: 16 }}>
                                <div>
                                    <Typography.Text>{name}</Typography.Text>
                                </div>
                            </div>
                        </Button>
                        <Tag>{totalExecutions || 0} runs</Tag>
                    </SourceHeaderContainer>
                    <div>
                        <Typography.Text code>{cron}</Typography.Text>
                    </div>
                    <div>
                        <Button onClick={onExecute}>EXECUTE</Button>
                        <Button
                            onClick={() => handleRemoveIngestionSource(source.urn)}
                            type="text"
                            shape="circle"
                            danger
                        >
                            <DeleteOutlined />
                        </Button>
                    </div>
                </SourceItemContainer>
            </List.Item>
        </>
    );
}
