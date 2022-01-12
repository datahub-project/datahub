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

export function FindMyUrn() {
    const { loading, data } = useQuery(GetMeOnlyDocument);
    if (loading) return '';
    const ans = data.me.corpUser.urn;
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
    console.log(`gettoken: ${userUrn} is the ident.`);
    const input = userUrn === '' ? 'urn:li:corpuser:datahub' : userUrn;
    const { data, loading, error } = useQuery(queryresult, {
        variables: {
            input: {
                type: 'PERSONAL',
                actorUrn: input,
                duration: 'ONE_HOUR',
            },
        },
        skip: input === 'urn:li:corpuser:datahub',
    });
    if (error) return 'error...';
    if (loading) return 'Loading...';
    return data?.getAccessToken?.accessToken;
    // need to use skip else it will keep attempting to query with incomplete info
    // which leads to <Unauthorised User> pop up in UI.
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
