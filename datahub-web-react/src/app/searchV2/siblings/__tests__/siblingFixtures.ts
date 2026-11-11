import { act } from '@testing-library/react';

import { SiblingGroupData, SiblingMemberData } from '@app/searchV2/siblings/useSiblingGroups';

import { EntityType } from '@types';

export const RESULT_URN = 'urn:li:dataset:(urn:li:dataPlatform:glue,db.orders,PROD)';

export function member(name: string, isPrimary = false): SiblingMemberData {
    return {
        __typename: 'SiblingMember',
        isPrimary,
        entity: {
            __typename: 'Dataset',
            urn: `urn:li:dataset:(urn:li:dataPlatform:glue,db.${name},PROD)`,
            type: EntityType.Dataset,
            name,
            platform: null,
            properties: { __typename: 'DatasetProperties', name },
            container: null,
        },
    } as unknown as SiblingMemberData;
}

export function group(members: SiblingMemberData[], total = members.length, urn = RESULT_URN): SiblingGroupData {
    return { __typename: 'SiblingGroup', urn, total, truncated: false, members } as SiblingGroupData;
}

/**
 * Lets pending Apollo work run (the mock link answers on a later tick), for checks that something
 * was NOT requested. Pair it with a positive check in the same test where one exists.
 */
export async function settle() {
    await act(async () => {
        await new Promise((resolve) => {
            setTimeout(resolve, 0);
        });
    });
}
