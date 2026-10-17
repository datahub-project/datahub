import { Text, Tooltip } from '@components';
import i18next from 'i18next';
import React, { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useDocumentPermissions } from '@app/document/hooks/useDocumentPermissions';
import { useUpdateDocument } from '@app/document/hooks/useUpdateDocument';
import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import BaseProperty from '@app/entityV2/summary/properties/property/properties/BaseProperty';
import { PropertyComponentProps } from '@app/entityV2/summary/properties/types';
import { SimpleSelect } from '@src/alchemy-components';

import { Document, DocumentState } from '@types';

const PUBLISHED_LIFECYCLE_STAGE_URN = 'urn:li:lifecycleStageType:PUBLISHED';
const DRAFT_LIFECYCLE_STAGE_URN = 'urn:li:lifecycleStageType:DRAFT';
const UNPUBLISHED_LIFECYCLE_STAGE_URN = 'urn:li:lifecycleStageType:UNPUBLISHED';

const STATUS_OPTIONS = [
    {
        get label() {
            return i18next.t('entity.profile.summary:documentStatus.published');
        },
        value: PUBLISHED_LIFECYCLE_STAGE_URN,
    },
    {
        get label() {
            return i18next.t('entity.profile.summary:documentStatus.draft');
        },
        value: DRAFT_LIFECYCLE_STAGE_URN,
    },
    {
        get label() {
            return i18next.t('entity.profile.summary:documentStatus.unpublished');
        },
        value: UNPUBLISHED_LIFECYCLE_STAGE_URN,
    },
];

function legacyStateToLifecycleUrn(state: DocumentState | null | undefined): string | null {
    if (state === DocumentState.Published) return PUBLISHED_LIFECYCLE_STAGE_URN;
    if (state === DocumentState.Unpublished) return UNPUBLISHED_LIFECYCLE_STAGE_URN;
    return null;
}

function urnToLabel(urn: string | null | undefined): string | null {
    const option = STATUS_OPTIONS.find((o) => o.value === urn);
    return option?.label ?? null;
}

const StatusSelectWrapper = styled.div``;

export default function DocumentStatusProperty(props: PropertyComponentProps) {
    const { t } = useTranslation('entity.profile.summary');
    const { urn, entityData } = useEntityData();
    const document = entityData as Document;
    const refetch = useRefetch();
    const { canEditState } = useDocumentPermissions(urn);
    const { updateLifecycleStage } = useUpdateDocument();

    // Prefer lifecycle stage URN from generic status aspect; fall back to legacy field
    const lifecycleUrn = (document as any)?.status?.lifecycleStage?.urn as string | null | undefined;
    const legacyState = document?.info?.status?.state;
    const serverStageUrn = lifecycleUrn ?? legacyStateToLifecycleUrn(legacyState);

    const [optimisticStageUrn, setOptimisticStageUrn] = useState<string | null | undefined>(serverStageUrn);

    useEffect(() => {
        setOptimisticStageUrn(serverStageUrn);
    }, [serverStageUrn]);

    const handleStatusChange = async (values: string[]) => {
        const newStageUrn = values[0] ?? null;
        const previousStageUrn = optimisticStageUrn;

        setOptimisticStageUrn(newStageUrn);

        try {
            await updateLifecycleStage({
                urn,
                lifecycleStageUrn: newStageUrn,
            });
            await refetch();
        } catch (error) {
            console.error('[DocumentStatusProperty] Update failed, reverting to:', previousStageUrn);
            setOptimisticStageUrn(previousStageUrn);
        }
    };

    const renderValue = () => {
        const label = urnToLabel(optimisticStageUrn);
        if (!label) return <span>-</span>;

        if (!canEditState) {
            return <Text>{label}</Text>;
        }

        return (
            <StatusSelectWrapper>
                <Tooltip title={t('documentStatus.publishTooltip')} placement="top">
                    <div data-testid="document-status-select">
                        <SimpleSelect
                            values={optimisticStageUrn ? [optimisticStageUrn] : []}
                            onUpdate={handleStatusChange}
                            isDisabled={false}
                            options={STATUS_OPTIONS}
                            size="sm"
                            width="fit-content"
                            showClear={false}
                        />
                    </div>
                </Tooltip>
            </StatusSelectWrapper>
        );
    };

    return (
        <BaseProperty
            {...props}
            values={optimisticStageUrn ? [optimisticStageUrn] : []}
            renderValue={renderValue}
            maxValues={1}
        />
    );
}
