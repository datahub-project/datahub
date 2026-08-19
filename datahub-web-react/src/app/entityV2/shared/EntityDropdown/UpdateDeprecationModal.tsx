import { Button, DatePicker, Loader, Modal, SimpleSelect, TextArea, toast } from '@components';
import React, { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { EntityCapabilityType } from '@app/entityV2/Entity';
import { downgradeV2FieldPath } from '@app/entityV2/dataset/profile/schema/utils/utils';
import { SearchSelectModal } from '@app/entityV2/shared/components/styled/search/SearchSelectModal';
import { useGetEntityWithSchema } from '@app/entityV2/shared/tabs/Dataset/Schema/useGetEntitySchema';
import { generateSchemaFieldUrn } from '@app/entityV2/shared/tabs/Lineage/utils';
import { handleBatchError } from '@app/entityV2/shared/utils';
import { EntityLink } from '@app/homeV2/reference/sections/EntityLink';
import { getV1FieldPathFromSchemaFieldUrn } from '@app/lineageV3/utils/lineageUtils';
import { decommissionTimeToSeconds } from '@app/shared/time/timeUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';
import type { Dayjs } from '@utils/dayjs';
import dayjs from '@utils/dayjs';

import { useGetEntitiesQuery } from '@graphql/entity.generated';
import { useBatchUpdateDeprecationMutation } from '@graphql/mutations.generated';
import { Deprecation, Entity, ResourceRefInput, SubResourceType } from '@types';

type DeprecationModalResult = {
    note?: string | null;
    decommissionTime?: number | null;
    replacement?: Entity | null;
};

type Props = {
    urns: string[];
    // if you need to provide context for subresources, resourceRefs should be provided and will take precedence over urns
    resourceRefs?: ResourceRefInput[];
    initialDeprecation?: Deprecation | null;
    onClose: () => void;
    refetch?: (result?: DeprecationModalResult) => void;
    zIndexOverride?: number;
};

const SCHEMA_FIELD_PREFIX = 'urn:li:schemaField:';

const getInitialFormValues = (initialDeprecation?: Deprecation | null) => ({
    note: initialDeprecation?.note ?? '',
    decommissionTime: initialDeprecation?.decommissionTime
        ? dayjs.unix(decommissionTimeToSeconds(initialDeprecation.decommissionTime))
        : undefined,
    replacementUrn: initialDeprecation?.replacement?.urn ?? null,
});

const FieldGroup = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

const ReplacementControls = styled.div`
    align-self: flex-start;
`;

export const UpdateDeprecationModal = ({
    urns,
    resourceRefs,
    initialDeprecation,
    onClose,
    refetch,
    zIndexOverride,
}: Props) => {
    const { t } = useTranslation('entity.shared.entityDropdown');
    const { t: tc } = useTranslation('common.actions');
    const { t: tcf } = useTranslation('common.feedback');
    const { entityWithSchema } = useGetEntityWithSchema();
    const schemaMetadata: any = entityWithSchema?.schemaMetadata || undefined;
    const entityRegistry = useEntityRegistry();
    const isEditMode = !!initialDeprecation;

    const [batchUpdateDeprecation] = useBatchUpdateDeprecationMutation();
    const [isReplacementModalVisible, setIsReplacementModalVisible] = useState(false);
    const initialFormValues = getInitialFormValues(initialDeprecation);
    const [replacementUrn, setReplacementUrn] = useState<string | null>(initialFormValues.replacementUrn);
    const [note, setNote] = useState<string>(initialFormValues.note);
    const [decommissionTime, setDecommissionTime] = useState<Dayjs | null | undefined>(
        initialFormValues.decommissionTime,
    );

    const isDeprecatingFields =
        !!resourceRefs && resourceRefs.length > 0 && resourceRefs[0].subResourceType === SubResourceType.DatasetField;
    const resourceFromWhichReplacementIsSelected = resourceRefs?.[0]?.resourceUrn;

    const { data: replacementData, loading: replacementLoading } = useGetEntitiesQuery({
        variables: {
            urns: [replacementUrn || ''],
        },
        skip: !replacementUrn || replacementUrn?.startsWith(SCHEMA_FIELD_PREFIX),
    });

    useEffect(() => {
        const nextValues = getInitialFormValues(initialDeprecation);
        setNote(nextValues.note);
        setDecommissionTime(nextValues.decommissionTime);
        setReplacementUrn(nextValues.replacementUrn);
    }, [initialDeprecation]);

    const handleSubmit = async () => {
        toast.loading(tcf('updating'));
        try {
            await batchUpdateDeprecation({
                variables: {
                    input: {
                        resources: resourceRefs || urns.map((resourceUrn) => ({ resourceUrn })),
                        deprecated: true,
                        note,
                        decommissionTime: decommissionTime ? decommissionTime.unix() * 1000 : null,
                        replacement: replacementUrn,
                    },
                },
            });
            analytics.event({
                type: EventType.SetDeprecation,
                entityUrns: urns,
                deprecated: true,
                resources: isDeprecatingFields ? resourceRefs : undefined,
            });
            toast.destroy();
            toast.success(isEditMode ? t('deprecation.updated') : t('deprecation.markedDeprecatedSuccess'), {
                duration: 2,
            });
        } catch (e: unknown) {
            toast.destroy();
            if (e instanceof Error) {
                const fallback = {
                    content: t('deprecation.updateError', { errorMessage: e.message || '' }),
                    duration: 2,
                };
                const { content, duration } = handleBatchError(urns, e, fallback);
                toast.error(content, { duration });
            }
        }
        refetch?.({
            note: note || null,
            decommissionTime: decommissionTime ? decommissionTime.unix() * 1000 : null,
            replacement: replacementData?.entities?.[0] ?? null,
        });
        onClose();
    };

    return (
        <Modal
            title={isEditMode ? t('deprecation.editTitle') : t('deprecation.modalTitle')}
            zIndex={zIndexOverride ?? 1000}
            onCancel={onClose}
            buttons={[
                {
                    text: tc('cancel'),
                    variant: 'text',
                    onClick: onClose,
                },
                {
                    buttonDataTestId: 'add-deprecation-submit',
                    text: isEditMode ? tc('save') : t('deprecation.ok'),
                    onClick: handleSubmit,
                },
            ]}
        >
            <FieldGroup>
                <TextArea
                    label={t('deprecation.reasonLabel')}
                    placeholder={t('deprecation.reasonPlaceholder')}
                    value={note}
                    onChange={(e) => setNote(e.target.value)}
                    rows={4}
                    autoFocus
                />
                <DatePicker
                    key={initialDeprecation?.decommissionTime ?? 'new-deprecation'}
                    placeholder={t('deprecation.decommissionDateLabel')}
                    value={decommissionTime}
                    onChange={(v) => setDecommissionTime(v)}
                />

                {isReplacementModalVisible && !isDeprecatingFields && (
                    <SearchSelectModal
                        limit={1}
                        titleText={t('deprecation.replacementSearchTitle')}
                        continueText={t('deprecation.setReplacement')}
                        onContinue={(entityUrns) => {
                            if (entityUrns.length > 0) {
                                setReplacementUrn(entityUrns[0]);
                            }
                            setIsReplacementModalVisible(false);
                        }}
                        onCancel={() => setIsReplacementModalVisible(false)}
                        fixedEntityTypes={Array.from(
                            entityRegistry.getTypesWithSupportedCapabilities(EntityCapabilityType.DEPRECATION),
                        )}
                    />
                )}
                {isReplacementModalVisible && isDeprecatingFields && (
                    <Modal
                        title={t('deprecation.selectReplacement')}
                        onCancel={() => setIsReplacementModalVisible(false)}
                        buttons={[
                            {
                                text: tc('cancel'),
                                variant: 'text',
                                onClick: () => setIsReplacementModalVisible(false),
                            },
                            {
                                text: tc('save'),
                                onClick: () => setIsReplacementModalVisible(false),
                            },
                        ]}
                    >
                        <SimpleSelect
                            placeholder={t('deprecation.selectReplacement')}
                            options={
                                schemaMetadata?.fields?.map((field: any) => ({
                                    value: field.fieldPath,
                                    label: downgradeV2FieldPath(field.fieldPath),
                                })) || []
                            }
                            onUpdate={(vals) => {
                                if (vals.length > 0) {
                                    setReplacementUrn(
                                        generateSchemaFieldUrn(vals[0], resourceFromWhichReplacementIsSelected || ''),
                                    );
                                }
                            }}
                        />
                    </Modal>
                )}

                <ReplacementControls>
                    {replacementUrn && replacementLoading && <Loader size="sm" />}
                    {replacementUrn && !replacementLoading && !!replacementData?.entities?.[0] && (
                        <EntityLink
                            onClick={() => setIsReplacementModalVisible(true)}
                            entity={replacementData?.entities?.[0] as any}
                        />
                    )}
                    {replacementUrn && isDeprecatingFields && (
                        <Button variant="text" onClick={() => setIsReplacementModalVisible(true)}>
                            {getV1FieldPathFromSchemaFieldUrn(replacementUrn)}
                        </Button>
                    )}
                    {!replacementUrn && (
                        <Button variant="secondary" size="sm" onClick={() => setIsReplacementModalVisible(true)}>
                            {t('deprecation.selectReplacement')}
                        </Button>
                    )}
                </ReplacementControls>
            </FieldGroup>
        </Modal>
    );
};
