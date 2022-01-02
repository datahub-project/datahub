// import * as React from 'react';
import { gql, useQuery } from '@apollo/client';
// import { useGetAccessTokenLazyQuery } from '../../../graphql/auth.generated';
// import { GetDatasetOwnersGqlDocument } from '../../../graphql/dataset.generated';
import { GetMeOnlyDocument } from '../../../graphql/me.generated';
// import { AccessTokenDuration, AccessTokenType } from '../../../types.generated';

export function FindWhoAmI() {
    const { loading, data } = useQuery(GetMeOnlyDocument);
    if (loading) return 'loading..';
    const ans = data.me.corpUser.username;
    return ans;
}
export function GetMyToken(userUrn: string) {
    const queryresult = gql`
        query getAccessToken($input: GetAccessTokenInput!) {
            getAccessToken(input: $input) {
                accessToken
            }
        }
    `;
    const { data, loading } = useQuery(queryresult, {
        variables: {
            input: {
                type: 'PERSONAL',
                actorUrn: userUrn,
                duration: 'ONE_HOUR',
            },
        },
    });
    if (loading) return 'Loading...';
    return data?.getAccessToken?.accessToken;
}
// export function FindOwners(dataset) {
//     console.log(`i call upon ${dataset}`);
//     const { data, loading } = useQuery(GetDatasetOwnersGqlDocument, {
//         variables: {
//             urn: dataset,
//         },
//     });
//     // if (error) return 'error';
//     const random = data?.dataset?.platform?.urn;
//     console.log(`received owners ${random}`);
//     if (loading) return 'still loading..';

//     const owners = data?.dataset?.ownership?.owners;

//     const ownersArray =
//         owners
//             ?.map((x) => (x?.type === 'DATAOWNER' && x?.owner?.__typename === 'CorpUser' ? x?.owner?.username : ''))
//             ?.flat() ?? [];
//     console.log(`ownership array is ${ownersArray.length} `);
//     return ownersArray;
// }
