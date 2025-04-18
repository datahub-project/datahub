import React, { useContext } from 'react';
import ManageFormContext from './ManageFormContext';
import { ListContainer } from './styledComponents';
import UserOrGroupItem from './UserOrGroupItem';

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
