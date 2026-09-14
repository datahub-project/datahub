import { Button, DatePicker, DatePickerVariant, Loader, Modal, TextArea, toast } from '@components';
import React, { useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { EntityCapabilityType } from '@app/entityV2/Entity';
import { getRawFieldPathFromSchemaFieldUrn, getSourceUrnFromSchemaFieldUrn } from '@app/entityV2/schemaField/utils';
import SelectColumnReplacementModal from '@app/entityV2/shared/EntityDropdown/SelectColumnReplacementModal';
import { SearchSelectModal } from '@app/entityV2/shared/components/styled/search/SearchSelectModal';
import { handleBatchError } from '@app/entityV2/shared/utils';
import { EntityLink } from '@app/homeV2/reference/sections/EntityLink';
import { getEntityTypeFromEntityUrn, getV1FieldPathFromSchemaFieldUrn } from '@app/lineageV3/utils/lineageUtils';
import { decommissionTimeToSeconds } from '@app/shared/time/timeUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';
import type { Dayjs } from '@utils/dayjs';
import dayjs from '@utils/dayjs';

import { useGetEntitiesQuery } from '@graphql/entity.generated';
import { useBatchUpdateDeprecationMutation } from '@graphql/mutations.generated';
import { Deprecation, Entity, EntityType, ResourceRefInput, SubResourceType } from '@types';

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
    const deprecatedFieldTableUrn = resourceRefs?.[0]?.resourceUrn;
    // Datasets and glossary terms both carry columns, and a replacement is only offered from the
    // same kind of parent, so the picker follows whatever the deprecated column hangs off.
    const deprecatedFieldParentType =
        (deprecatedFieldTableUrn && getEntityTypeFromEntityUrn(deprecatedFieldTableUrn, entityRegistry)) ||
        EntityType.Dataset;
    const replacementColumnUrn =
        isDeprecatingFields && replacementUrn?.startsWith(SCHEMA_FIELD_PREFIX) ? replacementUrn : undefined;

    // Both lookups here feed a display name and nothing else, so neither asks for the lineage counts
    // or the sibling search that getEntities would otherwise resolve.
    const { data: replacementData, loading: replacementLoading } = useGetEntitiesQuery({
        variables: {
            urns: [replacementUrn || ''],
            skipLineage: true,
            skipSiblingsSearch: true,
        },
        skip: !replacementUrn || replacementUrn?.startsWith(SCHEMA_FIELD_PREFIX),
    });

    // The table holding the replacement column, which may differ from the deprecated column's own.
    const replacementColumnTableUrn = replacementColumnUrn
        ? getSourceUrnFromSchemaFieldUrn(replacementColumnUrn)
        : undefined;

    const { data: replacementColumnTableData } = useGetEntitiesQuery({
        variables: {
            urns: [replacementColumnTableUrn || ''],
            skipLineage: true,
            skipSiblingsSearch: true,
        },
        skip: !replacementColumnTableUrn,
    });
    const replacementColumnTable = replacementColumnTableData?.entities?.[0];

    // A bare field path is ambiguous now that the replacement can live in any table, so name both.
    const replacementColumnLabel = useMemo(() => {
        if (!replacementColumnUrn) return '';
        const fieldPath = getV1FieldPathFromSchemaFieldUrn(replacementColumnUrn);
        if (!replacementColumnTable) return fieldPath;
        return `${entityRegistry.getDisplayName(replacementColumnTable.type, replacementColumnTable)}.${fieldPath}`;
    }, [replacementColumnUrn, replacementColumnTable, entityRegistry]);

    useEffect(() => {
        const nextValues = getInitialFormValues(initialDeprecation);
        setNote(nextValues.note);
        setDecommissionTime(nextValues.decommissionTime);
        setReplacementUrn(nextValues.replacementUrn);
    }, [initialDeprecation]);

    // The entity query above is skipped for schemaField urns, so a column replacement has to be
    // assembled from what the picker already knows. Anything else — including an asset replacement
    // an existing field deprecation may carry — is reported as the entity that was fetched.
    const replacementEntity: Entity | null = useMemo(() => {
        if (!replacementUrn) return null;
        if (replacementColumnUrn) return { urn: replacementColumnUrn, type: EntityType.SchemaField };
        return replacementData?.entities?.[0] ?? null;
    }, [replacementColumnUrn, replacementUrn, replacementData]);

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
            replacement: replacementEntity,
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
                    variant={DatePickerVariant.EditableInput}
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
                    <SelectColumnReplacementModal
                        parentEntityType={deprecatedFieldParentType}
                        initialTableUrn={replacementColumnTableUrn ?? deprecatedFieldTableUrn}
                        initialFieldPath={
                            replacementColumnUrn ? getRawFieldPathFromSchemaFieldUrn(replacementColumnUrn) : undefined
                        }
                        onSave={(nextReplacementUrn) => {
                            setReplacementUrn(nextReplacementUrn);
                            setIsReplacementModalVisible(false);
                        }}
                        onCancel={() => setIsReplacementModalVisible(false)}
                    />
                )}

                <ReplacementControls>
                    {replacementUrn && replacementLoading && <Loader size="sm" />}
                    {replacementUrn && !replacementLoading && !!replacementData?.entities?.[0] && (
                        <EntityLink
                            onClick={() => setIsReplacementModalVisible(true)}
                            entity={replacementData?.entities?.[0] as any}
                        />
                    )}
                    {!!replacementColumnUrn && (
                        <Button
                            variant="text"
                            onClick={() => setIsReplacementModalVisible(true)}
                            data-testid="edit-replacement"
                        >
                            {replacementColumnLabel}
                        </Button>
                    )}
                    {!replacementUrn && (
                        <Button
                            variant="secondary"
                            size="sm"
                            onClick={() => setIsReplacementModalVisible(true)}
                            data-testid="select-replacement"
                        >
                            {t('deprecation.selectReplacement')}
                        </Button>
                    )}
                </ReplacementControls>
            </FieldGroup>
        </Modal>
    );
};
