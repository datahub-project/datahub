import { Button, toast } from '@components';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
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
                toast.success(t('multiStep.secret.createSuccess'));
                setTimeout(() => refetchSecrets(), 3000);
            })
            .catch((e) => {
                toast.destroy();
                toast.error(t('multiStep.secret.createError', { error: e.message || '' }));
            });
    };

    return (
        <>
            <CreateButton onClick={() => setIsCreateModalVisible(true)} variant="text" icon={{ icon: Plus }}>
                {t('multiStep.secret.createButton')}
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
