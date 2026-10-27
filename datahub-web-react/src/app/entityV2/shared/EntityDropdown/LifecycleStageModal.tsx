import { Modal, SimpleSelect, Text, toast } from '@components';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { getSelectableStages } from '@app/entityV2/shared/EntityDropdown/utils';
import LifecycleStageBadge from '@app/entityV2/shared/containers/profile/header/LifecycleStageBadge';

import { useListLifecycleStagesQuery, useSetLifecycleStageMutation } from '@graphql/lifecycle.generated';
import { EntityType } from '@types';

const Body = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

const CurrentRow = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const ErrorText = styled.div`
    color: ${(props) => props.theme.colors.textError};
`;

type CurrentStage = { urn: string; name: string; description?: string | null };

type Props = {
    urn: string;
    entityType: EntityType;
    currentStage?: CurrentStage | null;
    onClose: () => void;
    onSuccess: () => void;
};

export default function LifecycleStageModal({ urn, entityType, currentStage, onClose, onSuccess }: Props) {
    const { t } = useTranslation('entity.shared.entityDropdown');
    const { t: tc } = useTranslation('common.actions');
    const [selectedStageUrn, setSelectedStageUrn] = useState<string | undefined>();
    const [submitError, setSubmitError] = useState<string | undefined>();
    const [isSubmitting, setIsSubmitting] = useState(false);

    const { data, loading, error: stagesError } = useListLifecycleStagesQuery({ variables: { entityType } });
    const [setStage] = useSetLifecycleStageMutation();

    const options = getSelectableStages(data?.listLifecycleStages ?? [], currentStage?.urn).map((stage) => ({
        label: stage.name,
        value: stage.urn,
    }));
    const canSubmit = !!selectedStageUrn && !isSubmitting;

    const submit = async () => {
        if (!selectedStageUrn) return;
        setIsSubmitting(true);
        setSubmitError(undefined);
        try {
            await setStage({
                variables: { urn, lifecycleStageUrn: selectedStageUrn },
                // The History sidebar caches the timeline; drop it so the stage change shows on next read.
                update: (cache) => {
                    cache.evict({ id: 'ROOT_QUERY', fieldName: 'getTimeline' });
                },
            });
            toast.success(t('lifecycleStage.setSuccess'));
            onSuccess();
            onClose();
        } catch (e) {
            setSubmitError(e instanceof Error ? e.message : String(e));
        } finally {
            setIsSubmitting(false);
        }
    };

    return (
        <Modal
            title={t('lifecycleStage.change')}
            onCancel={onClose}
            buttons={[
                { text: tc('cancel'), variant: 'text', onClick: onClose, key: 'cancel' },
                {
                    text: tc('save'),
                    variant: 'filled',
                    onClick: submit,
                    key: 'submit',
                    disabled: !canSubmit,
                    buttonDataTestId: 'lifecycle-stage-submit',
                },
            ]}
        >
            <Body>
                <CurrentRow>
                    <Text weight="semiBold">{t('lifecycleStage.current')}</Text>
                    {currentStage ? (
                        <LifecycleStageBadge lifecycleStage={currentStage} />
                    ) : (
                        <Text>{t('lifecycleStage.noStage')}</Text>
                    )}
                </CurrentRow>
                {stagesError && <ErrorText>{t('lifecycleStage.loadError')}</ErrorText>}
                {!stagesError && !loading && options.length === 0 && <Text>{t('lifecycleStage.noOptions')}</Text>}
                {!stagesError && (loading || options.length > 0) && (
                    <SimpleSelect
                        label={t('lifecycleStage.newStage')}
                        placeholder={t('lifecycleStage.selectPlaceholder')}
                        options={options}
                        values={selectedStageUrn ? [selectedStageUrn] : []}
                        onUpdate={(values) => setSelectedStageUrn(values[0])}
                        showClear={false}
                        width="full"
                        dataTestId="lifecycle-stage-select"
                    />
                )}
                {submitError && <ErrorText>{submitError}</ErrorText>}
            </Body>
        </Modal>
    );
}
