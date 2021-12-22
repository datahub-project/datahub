// import { Empty } from 'antd';
import React from 'react';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import { EntityType } from '../../../../../../types.generated';
// import { useGetAuthenticatedUser } from '../../../../../useGetAuthenticatedUser';
import { FindWhoAmI } from '../../../../dataset/whoAmI';
import { useBaseEntity } from '../../../EntityContext';
import { EditPropertiesTableEditable } from './EditPropertiesTableEditable';

export const EditPropertiesTab = () => {
    const queryBase = useBaseEntity<GetDatasetQuery>()?.dataset?.ownership?.owners;
    // const currUser = useGetAuthenticatedUser()?.corpUser?.username || '-';
    const currUser = FindWhoAmI();
    const ownersArray =
        queryBase
            ?.map((x) =>
                x?.type === 'DATAOWNER' && x?.owner?.type === EntityType.CorpUser
                    ? x?.owner?.urn.split(':').slice(-1)
                    : '',
            )
            .flat() || [];
    // console.log(`ownersArray is ${ownersArray} and I am ${currUser}`);
    if (ownersArray.includes(currUser)) {
        return (
            <>
                <EditPropertiesTableEditable />
            </>
        );
    }
    return (
        <>
            <span>You need to be a dataowner of this dataset to make edits</span>
        </>
    );
};
