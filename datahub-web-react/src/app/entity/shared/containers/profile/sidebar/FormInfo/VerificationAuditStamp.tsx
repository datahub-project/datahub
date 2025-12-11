/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import dayjs from 'dayjs';
import React from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { getVerificationAuditStamp } from '@app/entity/shared/containers/profile/sidebar/FormInfo/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

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
