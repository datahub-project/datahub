/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { getUsers } from '@graphql-mock/fixtures/searchResult/userSearchResult';

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
