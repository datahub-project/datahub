import { red } from '@ant-design/colors';
import { message } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components/macro';

import analytics, { EventType } from '@app/analytics';
import { AccessTokenModal } from '@app/settingsV2/AccessTokenModal';
import { ACCESS_TOKEN_DURATIONS, getTokenExpireDate } from '@app/settingsV2/utils';
import { useEnterKeyListener } from '@app/shared/useEnterKeyListener';
import { Button, Input, Modal, SimpleSelect, Text } from '@src/alchemy-components';
import { colors } from '@src/alchemy-components/theme';

import { useCreateAccessTokenMutation } from '@graphql/auth.generated';
import { AccessTokenDuration, AccessTokenType, CreateAccessTokenInput } from '@types';

type Props = {
    /** Whether the modal is visible */
    visible: boolean;
    /** Whether this is for a remote executor (locks duration to NoExpiry) */
    forRemoteExecutor?: boolean;
    /** Callback when modal is closed */
    onClose: () => void;
    /** Callback after token is created */
    onCreateToken: () => void;
    /** The URN of the actor (user or service account) to create the token for */
    actorUrn?: string;
    /** Legacy prop - The URN of the current user (for backward compatibility) */
    currentUserUrn?: string;
    /** The type of token to create */
    tokenType?: AccessTokenType;
    /** Display name of the actor (for modal title) */
    actorDisplayName?: string;
};

const FormContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 24px;
`;

const FormGroup = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

const FormLabel = styled(Text)`
    color: ${colors.gray[600]};
`;

const FormDescription = styled(Text)`
    color: ${colors.gray[1700]};
`;

const ExpirationContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    gap: 8px;
`;

const ExpirationText = styled(Text)<{ $isWarning?: boolean }>`
    ${(props) => props.$isWarning && `color: ${red[5]};`}
`;

const ModalFooter = styled.div`
    display: flex;
    justify-content: flex-end;
    gap: 8px;
`;

export default function CreateTokenModal({
    visible,
    forRemoteExecutor,
    onClose,
    onCreateToken,
    actorUrn,
    currentUserUrn,
    tokenType = AccessTokenType.Personal,
    actorDisplayName,
}: Props) {
    // Support legacy currentUserUrn prop
    const resolvedActorUrn = actorUrn || currentUserUrn || '';

    const [tokenName, setTokenName] = useState('');
    const [tokenDescription, setTokenDescription] = useState('');
    const [selectedDuration, setSelectedDuration] = useState<AccessTokenDuration>(
        forRemoteExecutor ? AccessTokenDuration.NoExpiry : ACCESS_TOKEN_DURATIONS[2].duration,
    );
    const [selectedTokenDuration, setSelectedTokenDuration] = useState<AccessTokenDuration | null>(null);
    const [showAccessTokenModal, setShowAccessTokenModal] = useState(false);
    const [tokenNameError, setTokenNameError] = useState('');

    const [createAccessToken, { data }] = useCreateAccessTokenMutation();

    // Update duration if forRemoteExecutor changes
    useEffect(() => {
        if (forRemoteExecutor) {
            setSelectedDuration(AccessTokenDuration.NoExpiry);
        }
    }, [forRemoteExecutor]);

    useEffect(() => {
        if (data && data.createAccessToken?.accessToken) {
            setShowAccessTokenModal(true);
        }
    }, [data]);

    const onDetailModalClose = () => {
        setSelectedTokenDuration(null);
        setShowAccessTokenModal(false);
        onCreateToken();
        onClose();
    };

    const resetForm = () => {
        setTokenName('');
        setTokenDescription('');
        setSelectedDuration(forRemoteExecutor ? AccessTokenDuration.NoExpiry : ACCESS_TOKEN_DURATIONS[2].duration);
        setTokenNameError('');
    };

    const onModalClose = () => {
        resetForm();
        onClose();
    };

    const validateForm = (): boolean => {
        if (!tokenName.trim()) {
            setTokenNameError('Token name is required');
            return false;
        }
        if (tokenName.length > 50) {
            setTokenNameError('Token name must be 50 characters or less');
            return false;
        }
        setTokenNameError('');
        return true;
    };

    const onCreateNewToken = () => {
        if (!validateForm()) return;

        const input: CreateAccessTokenInput = {
            actorUrn: resolvedActorUrn,
            type: tokenType,
            duration: selectedDuration,
            name: tokenName,
            description: tokenDescription || undefined,
        };

        createAccessToken({ variables: { input } })
            .then(({ errors }) => {
                if (!errors) {
                    setSelectedTokenDuration(selectedDuration);
                    analytics.event({
                        type: EventType.CreateAccessTokenEvent,
                        accessTokenType: tokenType,
                        duration: selectedDuration,
                    });
                    resetForm();
                }
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: `Failed to create token: ${e.message || ''}`, duration: 3 });
                onModalClose();
            });
    };

    useEnterKeyListener({
        querySelectorToExecuteClick: '#createTokenButton',
    });

    const accessToken = data?.createAccessToken?.accessToken;
    const selectedExpiresInText = selectedTokenDuration && getTokenExpireDate(selectedTokenDuration);
    const hasSelectedNoExpiration = selectedDuration === AccessTokenDuration.NoExpiry;
    const showFormModal = visible && !showAccessTokenModal;

    // Generate modal title based on token type and actor
    const getModalTitle = () => {
        if (forRemoteExecutor) {
            return 'Create Token for Remote Executor';
        }
        if (actorDisplayName) {
            return `Create Access Token for ${actorDisplayName}`;
        }
        if (tokenType === AccessTokenType.ServiceAccount) {
            return 'Create Service Account Token';
        }
        return 'Create Access Token';
    };

    const durationOptions = ACCESS_TOKEN_DURATIONS.map((duration) => ({
        value: duration.duration,
        label: duration.text,
    }));

    return (
        <>
            {showFormModal && (
                <Modal
                    title={getModalTitle()}
                    onCancel={onModalClose}
                    dataTestId="create-token-modal"
                    footer={
                        <ModalFooter>
                            <Button
                                onClick={onModalClose}
                                variant="text"
                                color="gray"
                                data-testid="cancel-create-token-button"
                            >
                                Cancel
                            </Button>
                            <Button
                                id="createTokenButton"
                                onClick={onCreateNewToken}
                                disabled={!tokenName.trim()}
                                data-testid="create-access-token-button"
                            >
                                Create
                            </Button>
                        </ModalFooter>
                    }
                >
                    <FormContainer>
                        <FormGroup>
                            <FormLabel size="md" weight="semiBold">
                                Token Name
                            </FormLabel>
                            <FormDescription size="sm" color="gray">
                                Give this token a name to identify it.
                            </FormDescription>
                            <Input
                                value={tokenName}
                                setValue={setTokenName}
                                placeholder="A name for the token"
                                error={tokenNameError}
                                maxLength={50}
                                inputTestId="create-access-token-name"
                            />
                        </FormGroup>
                        <FormGroup>
                            <FormLabel size="md" weight="semiBold">
                                Description
                            </FormLabel>
                            <FormDescription size="sm" color="gray">
                                An optional description for this token.
                            </FormDescription>
                            <Input
                                value={tokenDescription}
                                setValue={setTokenDescription}
                                placeholder="A description for the token"
                                maxLength={500}
                                inputTestId="create-access-token-description"
                            />
                        </FormGroup>
                        <ExpirationContainer>
                            <FormLabel size="md" weight="semiBold">
                                Expires in
                            </FormLabel>
                            <SimpleSelect
                                options={durationOptions}
                                values={[selectedDuration]}
                                onUpdate={(values) => {
                                    if (values.length > 0) {
                                        setSelectedDuration(values[0] as AccessTokenDuration);
                                    }
                                }}
                                showClear={false}
                                width="full"
                                isDisabled={forRemoteExecutor}
                                dataTestId="create-token-duration"
                            />
                            <ExpirationText size="sm" color="gray" $isWarning={hasSelectedNoExpiration}>
                                {getTokenExpireDate(selectedDuration)}
                            </ExpirationText>
                        </ExpirationContainer>
                    </FormContainer>
                </Modal>
            )}
            <AccessTokenModal
                visible={showAccessTokenModal}
                onClose={onDetailModalClose}
                accessToken={accessToken || ''}
                expiresInText={selectedExpiresInText || ''}
            />
        </>
    );
}
