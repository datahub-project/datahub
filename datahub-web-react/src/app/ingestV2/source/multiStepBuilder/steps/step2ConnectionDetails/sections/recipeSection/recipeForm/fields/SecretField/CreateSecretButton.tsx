import { PlusOutlined } from '@ant-design/icons';
import { Button } from '@components';
import { message } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import { SecretBuilderModal } from '@app/ingestV2/secret/SecretBuilderModal';
import { SecretBuilderState } from '@app/ingestV2/secret/types';

import { useCreateSecretMutation } from '@graphql/ingestion.generated';

const CreateButton = styled(Button)`
    align-items: center;
    display: flex;
    justify-content: center;
    width: 100%;
    /* margin: 8px 12px 4px 12px; */
    /* width: calc(100% - 24px); */
    /* 
    &:hover {
        color: ${(props) => props.theme.colors.textBrand};
    }

    .anticon-plus {
        margin-right: 5px;
    } */
`;

interface Props {
    initialState?: SecretBuilderState;
    onSubmit?: (state: SecretBuilderState) => void;
    refetchSecrets: () => void;
}

function CreateSecretButton({ initialState, onSubmit, refetchSecrets }: Props) {
    const { t } = useTranslation('ingestion.sourceBuilder');
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
                onSubmit?.(state);
                setIsCreateModalVisible(false);
                resetBuilderState();
                message.success({ content: t('multiStep.secret.createSuccess') });
                setTimeout(() => refetchSecrets(), 3000);
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: t('multiStep.secret.createError', { error: e.message || '' }) });
            });
    };

    return (
        <>
            <CreateButton onClick={() => setIsCreateModalVisible(true)} variant="text">
                <PlusOutlined /> {t('multiStep.secret.createButton')}
            </CreateButton>
            {isCreateModalVisible && (
                <SecretBuilderModal
                    initialState={initialState}
                    open={isCreateModalVisible}
                    onCancel={() => setIsCreateModalVisible(false)}
                    onSubmit={createSecret}
                />
            )}
        </>
    );
}

export default CreateSecretButton;
