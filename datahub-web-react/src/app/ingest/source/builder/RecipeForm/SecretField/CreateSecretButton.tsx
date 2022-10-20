import React, { useState } from 'react';
import { Button, message } from 'antd';
import { PlusOutlined } from '@ant-design/icons';
import { blue } from '@ant-design/colors';
import styled from 'styled-components/macro';
import { SecretBuilderModal } from '../../../../secret/SecretBuilderModal';
import { useCreateSecretMutation } from '../../../../../../graphql/ingestion.generated';
import { SecretBuilderState } from '../../../../secret/types';

const CreateButton = styled(Button)`
    align-items: center;
    display: flex;
    justify-content: center;
    margin: 8px 12px 4px 12px;
    width: calc(100% - 24px);

    &:hover {
        color: ${blue[5]};
    }

    .anticon-plus {
        margin-right: 5px;
    }
`;

interface Props {
    refetchSecrets: () => void;
}

function CreateSecretButton({ refetchSecrets }: Props) {
    const [isCreateModalVisible, setIsCreateModalVisible] = useState(false);
    const [createSecretMutation] = useCreateSecretMutation();

    const createSecret = (state: SecretBuilderState, resetBuilderState: () => void) => {
        createSecretMutation({
            variables: {
                input: {
                    name: state.name as string,
                    value: state.value as string,
                    description: state.description as string,
                },
            },
        })
            .then(() => {
                setIsCreateModalVisible(false);
                resetBuilderState();
                setTimeout(() => refetchSecrets(), 3000);
                message.loading({ content: `Loading...`, duration: 3 }).then(() => {
                    message.success({ content: `Successfully created Secret!` });
                });
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: `Failed to create secret: \n ${e.message || ''}` });
            });
    };

    return (
        <>
            <CreateButton onClick={() => setIsCreateModalVisible(true)} type="text">
                <PlusOutlined /> Create Secret
            </CreateButton>
            {isCreateModalVisible && (
                <SecretBuilderModal
                    visible={isCreateModalVisible}
                    onCancel={() => setIsCreateModalVisible(false)}
                    onSubmit={createSecret}
                />
            )}
        </>
    );
}

export default CreateSecretButton;
