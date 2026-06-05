import React from 'react';
import { useTranslation } from 'react-i18next';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { getVerificationAuditStamp } from '@app/entity/shared/containers/profile/sidebar/FormInfo/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';
import dayjs from '@utils/dayjs';

interface Props {
    formUrn?: string;
}

export default function VerificationAuditStamp({ formUrn }: Props) {
    const { t } = useTranslation('entity.shared.containers');
    const entityRegistry = useEntityRegistry();
    const { entityData } = useEntityData();
    const verifiedAuditStamp = getVerificationAuditStamp(entityData, formUrn);
    const verifiedTimestamp = verifiedAuditStamp?.time;
    const verifiedActor = verifiedAuditStamp?.actor;

    if (!verifiedTimestamp) return null;

    const date = dayjs(verifiedTimestamp).format('ll');
    const actor = verifiedActor ? entityRegistry.getDisplayName(verifiedActor.type, verifiedActor) : undefined;

    return (
        <div>
            {actor
                ? t('formInfo.auditStamp.onDateByActor', { date, actor })
                : t('formInfo.auditStamp.onDate', { date })}
        </div>
    );
}
