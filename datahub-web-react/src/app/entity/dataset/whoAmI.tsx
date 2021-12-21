// import * as React from 'react';
import { useQuery } from '@apollo/client';
// import { GetDatasetOwnersGqlDocument } from '../../../graphql/dataset.generated';
import { GetMeOnlyDocument } from '../../../graphql/me.generated';

export function FindWhoAmI() {
    const { loading, data } = useQuery(GetMeOnlyDocument);
    if (loading) return 'loading..';
    const ans = data.me.corpUser.username;
    return ans;
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
