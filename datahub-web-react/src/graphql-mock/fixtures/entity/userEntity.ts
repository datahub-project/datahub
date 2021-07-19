import * as faker from 'faker';
import { CorpUser, EntityType } from '../../../types.generated';

export type UserEntityArg = {
    username: string;
};

export const userEntity = (option?: UserEntityArg): CorpUser => {
    const username = option?.username || `${faker.internet.userName()}`;
    const title = `${faker.name.title()}`;
    const firstName = `${faker.name.firstName()}`;
    const lastName = `${faker.name.lastName()}`;
    const email = `${faker.internet.email()}`;

    return {
        urn: `urn:li:corpuser:${username}`,
        type: EntityType.CorpUser,
        username,
        info: {
            active: true,
            displayName: `${firstName} ${lastName}`,
            title,
            email,
            firstName,
            lastName,
            fullName: `${firstName} ${lastName}`,
            __typename: 'CorpUserInfo',
        },
        editableInfo: {
            aboutMe: `about ${username}`,
            teams: [faker.company.companyName(), faker.company.companyName()],
            skills: [faker.company.catchPhrase(), faker.company.catchPhrase()],
            pictureLink: null,
            __typename: 'CorpUserEditableInfo',
        },
        __typename: 'CorpUser',
    };
};
