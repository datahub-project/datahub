import React, { useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import analytics, { EventType } from '@app/analytics';
import { AccessTokenModal } from '@app/settingsV2/AccessTokenModal';
import {
    ACCESS_TOKEN_NO_EXPIRY,
    DEFAULT_ACCESS_TOKEN_DURATIONS_ISO,
    buildAccessTokenDurationOptions,
    getDefaultAccessTokenDuration,
    getTokenExpireDate,
} from '@app/settingsV2/utils';
import { useEnterKeyListener } from '@app/shared/useEnterKeyListener';
import { useAppConfig } from '@app/useAppConfig';
import { Button, Input, Modal, SimpleSelect, Text, TextArea, toast } from '@src/alchemy-components';
import { spacing } from '@src/alchemy-components/theme';

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
    gap: ${spacing.lg};
`;

const ExpirationContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    gap: ${spacing.xsm};
`;

const ExpirationText = styled(Text)<{ $isWarning?: boolean }>`
    ${(props) => props.$isWarning && props.theme?.colors?.textError && `color: ${props.theme.colors.textError};`}
`;

const ModalFooter = styled.div`
    display: flex;
    justify-content: flex-end;
    gap: ${spacing.xsm};
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
    const { t } = useTranslation('settings.tokens');
    const { t: tc } = useTranslation('common.actions');
    const { config } = useAppConfig();
    const allowedIsoDurations = useMemo(
        () => config.authConfig?.allowedAccessTokenDurations ?? [...DEFAULT_ACCESS_TOKEN_DURATIONS_ISO],
        [config.authConfig?.allowedAccessTokenDurations],
    );
    const allowNoExpiry = config.authConfig?.allowNoExpiry ?? false;
    // Remote executor historically required Never; only lock to that when never-expire is allowed.
    const lockToNoExpiry = Boolean(forRemoteExecutor && allowNoExpiry);

    const durationOptions = useMemo(
        () => buildAccessTokenDurationOptions(allowedIsoDurations, allowNoExpiry),
        [allowedIsoDurations, allowNoExpiry],
    );

    const defaultDuration = lockToNoExpiry
        ? ACCESS_TOKEN_NO_EXPIRY
        : getDefaultAccessTokenDuration(allowedIsoDurations);

    // Support legacy currentUserUrn prop
    const resolvedActorUrn = actorUrn || currentUserUrn || '';

    const [tokenName, setTokenName] = useState('');
    const [tokenDescription, setTokenDescription] = useState('');
    const [selectedDuration, setSelectedDuration] = useState<string>(defaultDuration);
    const [selectedTokenDuration, setSelectedTokenDuration] = useState<string | null>(null);
    const [showAccessTokenModal, setShowAccessTokenModal] = useState(false);
    const [tokenNameError, setTokenNameError] = useState('');

    const [createAccessToken, { data }] = useCreateAccessTokenMutation();

    useEffect(() => {
        if (lockToNoExpiry) {
            setSelectedDuration(ACCESS_TOKEN_NO_EXPIRY);
        } else {
            setSelectedDuration(getDefaultAccessTokenDuration(allowedIsoDurations));
        }
    }, [lockToNoExpiry, allowedIsoDurations]);

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
        setSelectedDuration(
            lockToNoExpiry ? ACCESS_TOKEN_NO_EXPIRY : getDefaultAccessTokenDuration(allowedIsoDurations),
        );
        setTokenNameError('');
    };

    const onModalClose = () => {
        resetForm();
        onClose();
    };

    const validateForm = (): boolean => {
        if (!tokenName.trim()) {
            setTokenNameError(t('nameRequired'));
            return false;
        }
        if (tokenName.length > 50) {
            setTokenNameError(t('nameTooLong'));
            return false;
        }
        setTokenNameError('');
        return true;
    };

    const onCreateNewToken = () => {
        if (!validateForm()) return;

        const input: CreateAccessTokenInput =
            selectedDuration === ACCESS_TOKEN_NO_EXPIRY && allowNoExpiry
                ? {
                      actorUrn: resolvedActorUrn,
                      type: tokenType,
                      duration: AccessTokenDuration.NoExpiry,
                      name: tokenName,
                      description: tokenDescription || undefined,
                  }
                : {
                      actorUrn: resolvedActorUrn,
                      type: tokenType,
                      durationIso:
                          selectedDuration === ACCESS_TOKEN_NO_EXPIRY
                              ? getDefaultAccessTokenDuration(allowedIsoDurations)
                              : selectedDuration,
                      name: tokenName,
                      description: tokenDescription || undefined,
                  };

        createAccessToken({ variables: { input } })
            .then(({ errors }) => {
                if (!errors) {
                    toast.success(t('createSuccess'));
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
                toast.error(t('createError', { message: e.message || '' }));
                onModalClose();
            });
    };

    useEnterKeyListener({
        querySelectorToExecuteClick: '#createTokenButton',
    });

    const accessToken = data?.createAccessToken?.accessToken;
    const selectedExpiresInText = selectedTokenDuration && getTokenExpireDate(selectedTokenDuration);
    const hasSelectedNoExpiration = selectedDuration === ACCESS_TOKEN_NO_EXPIRY;
    const showFormModal = visible && !showAccessTokenModal;

    const getModalTitle = () => {
        if (forRemoteExecutor) {
            return t('createForRemoteExecutorTitle');
        }
        if (actorDisplayName) {
            return t('createForActorTitle', { name: actorDisplayName });
        }
        if (tokenType === AccessTokenType.ServiceAccount) {
            return t('createServiceAccountTitle');
        }
        return t('createTitle');
    };

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
                                {tc('cancel')}
                            </Button>
                            <Button
                                id="createTokenButton"
                                onClick={onCreateNewToken}
                                disabled={!tokenName.trim()}
                                data-testid="create-access-token-button"
                            >
                                {tc('create')}
                            </Button>
                        </ModalFooter>
                    }
                >
                    <FormContainer>
                        <Input
                            label={t('nameLabel')}
                            isRequired
                            value={tokenName}
                            setValue={setTokenName}
                            placeholder={t('namePlaceholder')}
                            error={tokenNameError}
                            maxLength={50}
                            inputTestId="create-access-token-name"
                        />
                        <TextArea
                            label={t('descriptionLabel')}
                            value={tokenDescription}
                            onChange={(e) => setTokenDescription(e.target.value)}
                            placeholder={t('descriptionPlaceholder')}
                            maxLength={500}
                            rows={3}
                            id="create-access-token-description"
                            data-testid="create-access-token-description"
                        />
                        <ExpirationContainer>
                            <SimpleSelect
                                label={t('expiresInLabel')}
                                options={durationOptions}
                                values={[selectedDuration]}
                                onUpdate={(values) => {
                                    if (values.length > 0) {
                                        setSelectedDuration(values[0]);
                                    }
                                }}
                                showClear={false}
                                width="full"
                                isDisabled={lockToNoExpiry}
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
