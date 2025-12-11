/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useGetEntities } from '@app/entity/shared/useGetEntities';

import { StringMapEntry } from '@types';

export function usePropagationDetails(sourceDetail?: StringMapEntry[] | null) {
    const isPropagated = !!sourceDetail?.find((mapEntry) => mapEntry.key === 'propagated' && mapEntry.value === 'true');
    const originEntityUrn = sourceDetail?.find((mapEntry) => mapEntry.key === 'origin')?.value || '';
    const viaEntityUrn = sourceDetail?.find((mapEntry) => mapEntry.key === 'via')?.value || '';

    const entities = useGetEntities([originEntityUrn, viaEntityUrn]);
    const originEntity = entities.find((e) => e.urn === originEntityUrn);
    const viaEntity = entities.find((e) => e.urn === viaEntityUrn);

    return {
        isPropagated,
        origin: {
            urn: originEntityUrn,
            entity: originEntity,
        },
        via: {
            urn: viaEntityUrn,
            entity: viaEntity,
        },
    };
}
