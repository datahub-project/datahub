// import * as React from 'react';
import { gql, useQuery } from '@apollo/client';
import { useGetMeQuery } from '../../../graphql/me.generated';
import { GetDatasetQuery } from '../../../graphql/dataset.generated';
import { EntityType } from '../../../types.generated';

export function FindWhoAmI() {
    const { data } = useGetMeQuery();
    return data?.me?.corpUser.username ?? '';
}

export function FindMyUrn() {
    const { data } = useGetMeQuery();
    return data?.me?.corpUser.urn ?? '';
}

export function FindMyGroups() {
    const currUserUrn = FindMyUrn();
    const queryresult = gql`
        query test($urn: String!) {
            corpUser(urn: $urn) {
                relationships(input: { types: "IsMemberOfGroup", direction: OUTGOING }) {
                    count
                    relationships {
                        entity {
                            urn
                        }
                    }
                }
            }
        }
    `;
    const { data, loading, error } = useQuery(queryresult, {
        variables: {
            urn: currUserUrn,
        },
        skip: currUserUrn === '',
    });
    if (error) return [];
    if (loading) return [];
    return data?.corpUser?.relationships?.relationships || [];
}

export function checkOwnership(data: GetDatasetQuery): boolean {
    const currUserUrn = FindMyUrn();
    const ownership = data?.dataset?.ownership?.owners;
    const individualOwnersArray =
        ownership?.map((x) => (x?.owner?.type === EntityType.CorpUser ? x?.owner?.urn : null)) || [];
    // console.log(`individualOwnersArray is ${individualOwnersArray}`);
    const groupOwnersArray =
        ownership?.map((x) => (x?.owner?.type === EntityType.CorpGroup ? x?.owner?.urn : null)) || [];
    // console.log(`groupOwnersArray is ${groupOwnersArray}`);
    const userGroups = FindMyGroups();
    // console.log(`userGroups is ${userGroups}`);
    const groupUrn = userGroups?.map((x) => x?.entity?.urn) || [];
    const intersection = groupUrn.filter((x) => groupOwnersArray.includes(x));
    // console.log(`groups intersection is ${intersection.length}`);
    return individualOwnersArray.includes(currUserUrn) || intersection.length > 0;
}

export function GetMyToken(userUrn: string) {
    const queryresult = gql`
        query getAccessToken($input: GetAccessTokenInput!) {
            getAccessToken(input: $input) {
                accessToken
            }
        }
    `;
    const input = userUrn === '' ? 'urn:li:corpuser:impossible' : userUrn;
    const { data, loading, error } = useQuery(queryresult, {
        variables: {
            input: {
                type: 'PERSONAL',
                actorUrn: input,
                duration: 'ONE_HOUR',
            },
        },
        skip: input === 'urn:li:corpuser:impossible',
        pollInterval: 1200000,
    });
    if (error) return 'error...';
    if (loading) return 'Loading...';
    return data?.getAccessToken?.accessToken;
    // need to use skip else it will keep attempting to query with incomplete info
    // which leads to <Unauthorised User> pop up in UI.
}

// export function GetMyToken2(userUrn: string) {
//     const MINUTE_MS = 5000;
//     const [token, setToken] = useState('');
//     useEffect(() => {
//     const interval = setInterval(() => {
//         console.log('Logs every minute');
//         const queryresult = gql`
//             query getAccessToken($input: GetAccessTokenInput!) {
//                 getAccessToken(input: $input) {
//                     accessToken
//                 }
//             }
//         `;
//         console.log(`gettoken: ${userUrn} is the ident.`);
//         const input = userUrn === '' ? 'urn:li:corpuser:impossible' : userUrn;
//         const { data, loading, error } = useQuery(queryresult, {
//             variables: {
//                 input: {
//                     type: 'PERSONAL',
//                     actorUrn: input,
//                     duration: 'ONE_HOUR',
//                 },
//             },
//             skip: input === 'urn:li:corpuser:impossible',
//         });
//         if (error) return 'error...';
//         if (loading) return 'Loading...';
//         setToken(data?.getAccessToken?.accessToken);
//     }, MINUTE_MS);

//     return () => clearInterval(interval); // This represents the unmount function, in which you need to clear your interval to prevent memory leaks.
//     }, [userUrn]);

//     return token;

//     // need to use skip else it will keep attempting to query with incomplete info
//     // which leads to <Unauthorised User> pop up in UI.
// }
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
