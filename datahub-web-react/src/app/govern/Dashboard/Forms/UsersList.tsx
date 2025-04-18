import React, { useContext } from 'react';

import ManageFormContext from '@app/govern/Dashboard/Forms/ManageFormContext';
import UserOrGroupItem from '@app/govern/Dashboard/Forms/UserOrGroupItem';
import { ListContainer } from '@app/govern/Dashboard/Forms/styledComponents';

const UsersList = () => {
    const { formValues } = useContext(ManageFormContext);

    return (
        <ListContainer>
            {formValues.actors?.users?.map((user) => {
                return <UserOrGroupItem userOrGroup={user} isGroup={false} />;
            })}

            {formValues.actors?.groups?.map((group) => {
                return <UserOrGroupItem userOrGroup={group} isGroup />;
            })}
        </ListContainer>
    );
};

export default UsersList;
