import React from 'react';
import styled from 'styled-components';
import { Button, List, message, Modal, Typography } from 'antd';
import { DeleteOutlined } from '@ant-design/icons';
import { useDeleteSecretMutation } from '../../graphql/ingestion.generated';
import { Secret } from '../../types.generated';

type Props = {
    secret: Secret;
    onClick?: () => void;
    onDelete?: () => void;
};

const ItemContainer = styled.div`
    display: flex;
    justify-content: space-between;
    padding-left: 8px;
    padding-right: 8px;
    width: 100%;
`;

const HeaderContainer = styled.div`
    display: flex;
    justify-content: left;
    align-items: center;
`;

export default function SecretListItem({ secret, onClick, onDelete }: Props) {
    const [deleteSecretMutation] = useDeleteSecretMutation();

    const onDeleteSecret = async (urn: string) => {
        try {
            await deleteSecretMutation({
                variables: { urn },
            });
            message.success({ content: 'Removed secret.', duration: 2 });
            onDelete?.();
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error({ content: `Failed to remove secret: \n ${e.message || ''}`, duration: 3 });
            }
        }
    };

    const handleRemoveSecret = (urn: string) => {
        Modal.confirm({
            title: `Confirm Secret Removal`,
            content: `Are you sure you want to remove this secret? Sources that use it may no longer work as expected.`,
            onOk() {
                onDeleteSecret(urn);
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    const { name } = secret;

    return (
        <List.Item>
            <ItemContainer>
                <Button type="text" onClick={onClick}>
                    <HeaderContainer>
                        <div style={{ marginLeft: 16, marginRight: 16 }}>
                            <div>
                                <Typography.Text>{name}</Typography.Text>
                            </div>
                        </div>
                    </HeaderContainer>
                </Button>
                <div>
                    <Button onClick={() => handleRemoveSecret(secret.urn)} type="text" shape="circle" danger>
                        <DeleteOutlined />
                    </Button>
                </div>
            </ItemContainer>
        </List.Item>
    );
}
