// import * as React from 'react';
import { gql, useQuery } from '@apollo/client';
import { GetMeOnlyDocument } from '../../../graphql/me.generated';
import { GetDatasetQuery } from '../../../graphql/dataset.generated';

export function FindWhoAmI() {
    const { loading, data } = useQuery(GetMeOnlyDocument);
    if (loading) return 'loading..';
    return data.me.corpUser.username;
}

export function checkOwnership(data: GetDatasetQuery): boolean {
    const currUser = FindWhoAmI();
    const ownership = data?.dataset?.ownership?.owners;
    const ownersArray =
        ownership?.map((x) =>
            x?.type === 'DATAOWNER' && x?.owner?.__typename === 'CorpUser' ? x?.owner?.username : '',
        ) || [];

    return ownersArray.includes(currUser);
}

export function FindMyUrn() {
    const { loading, data } = useQuery(GetMeOnlyDocument);
    if (loading) return '';
    return data.me.corpUser.urn;
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
