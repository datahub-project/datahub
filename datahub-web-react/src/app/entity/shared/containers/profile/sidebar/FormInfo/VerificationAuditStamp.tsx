import dayjs from 'dayjs';
import React from 'react';
import { useEntityRegistry } from '../../../../../../useEntityRegistry';
import { getVerificationAuditStamp } from './utils';
import { useEntityData } from '../../../../EntityContext';

interface Props {
    formUrn?: string;
}

export default function VerificationAuditStamp({ formUrn }: Props) {
    const entityRegistry = useEntityRegistry();
    const { entityData } = useEntityData();
    const verifiedAuditStamp = getVerificationAuditStamp(entityData, formUrn);
    const verifiedTimestamp = verifiedAuditStamp?.time;
    const verifiedActor = verifiedAuditStamp?.actor;

    if (!verifiedTimestamp) return null;

    return (
        <div>
            On {dayjs(verifiedTimestamp).format('ll')}{' '}
            {verifiedActor && <>by {entityRegistry.getDisplayName(verifiedActor.type, verifiedActor)}</>}
        </div>
    );
}
