import { getUsers } from './searchResult/userSearchResult';

const createCorpUserSchema = ({ server, user }) => {
    const { info, editableInfo } = user;

    // eslint-disable-next-line no-param-reassign
    delete user.info;
    // eslint-disable-next-line no-param-reassign
    delete user.editableInfo;

    const userSchema = server.create('CorpUser', user);
    userSchema.createInfo(info);
    userSchema.createEditableInfo(editableInfo);
};

export const createLoginUsers = (server) => {
    const users = getUsers();
    users.forEach((user) => {
        createCorpUserSchema({ server, user });
    });
};
