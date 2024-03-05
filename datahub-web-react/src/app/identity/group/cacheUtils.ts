import { ListGroupsDocument, ListGroupsQuery } from '../../../graphql/group.generated';

export const DEFAULT_GROUP_LIST_PAGE_SIZE = 25;

export const removeGroupFromListGroupsCache = (urn, client, page, pageSize) => {
    const currData: ListGroupsQuery | null = client.readQuery({
        query: ListGroupsDocument,
        variables: {
            input: {
                start: (page - 1) * pageSize,
                count: pageSize,
            },
        },
    });

    const newGroups = [...(currData?.listGroups?.groups || []).filter((source) => source.urn !== urn)];

    client.writeQuery({
        query: ListGroupsDocument,
        variables: {
            input: {
                start: (page - 1) * pageSize,
                count: pageSize,
            },
        },
        data: {
            listGroups: {
                start: currData?.listGroups?.start || 0,
                count: (currData?.listGroups?.count || 1) - 1,
                total: (currData?.listGroups?.total || 1) - 1,
                groups: newGroups,
            },
        },
    });
};

const createFullGroup = (baseGroup) => {
    return {
        ...baseGroup,
        origin: null,
        info: {
            ...baseGroup.info,
            displayName: baseGroup.name,
            email: null,
        },
        memberCount: null,
        roles: null,
        editableProperties: {
            pictureLink: null,
        },
    };
};

export const addGroupToListGroupsCache = (group, client) => {
    const currData: ListGroupsQuery | null = client.readQuery({
        query: ListGroupsDocument,
        variables: {
            input: {
                start: 0,
                count: DEFAULT_GROUP_LIST_PAGE_SIZE,
            },
        },
    });

    const newGroups = [createFullGroup(group), ...(currData?.listGroups?.groups || [])];

    client.writeQuery({
        query: ListGroupsDocument,
        variables: {
            input: {
                start: 0,
                count: DEFAULT_GROUP_LIST_PAGE_SIZE,
            },
        },
        data: {
            listGroups: {
                start: currData?.listGroups?.start || 0,
                count: (currData?.listGroups?.count || 1) + 1,
                total: (currData?.listGroups?.total || 1) + 1,
                groups: newGroups,
            },
        },
    });
};
