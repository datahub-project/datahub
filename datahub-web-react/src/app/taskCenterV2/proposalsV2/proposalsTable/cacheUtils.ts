import { ApolloClient } from '@apollo/client';

import { ActionRequestResult, ActionRequestStatus, CorpUser, EntityType } from '@types';

export const updateActionRequestsList = (
    client: ApolloClient<object>,
    requestUrns: string[],
    result: ActionRequestResult,
    resultNote: string,
    currentUser: CorpUser | null | undefined,
) => {
    client.cache.modify({
        fields: {
            listActionRequests(existingData, { readField, storeFieldName }) {
                const requests = existingData.actionRequests || [];
                const total = existingData.total ?? 0;

                // Extract the argument string from the query "listActionRequests" using regex.
                // Apollo stores cache keys like: listActionRequests({"input":{...}})
                const argsString = storeFieldName.match(/listActionRequests\((.*)\)/);

                // First group captures the arguments string inside the parentheses
                if (!argsString || !argsString[1]) return existingData;

                const args = JSON.parse(argsString[1]);
                const orFilters = args.input?.orFilters ?? [];

                // Check if the items needs to be removed, if the filter contains only pending status
                const shouldRemove = orFilters.some((filter) => {
                    const conditions = filter.and ?? [];

                    const includesPending = conditions.some(
                        (condition) =>
                            condition.field === 'status' && condition.values?.includes(ActionRequestStatus.Pending),
                    );

                    const includesCompleted = conditions.some(
                        (condition) =>
                            condition.field === 'status' && condition.values?.includes(ActionRequestStatus.Completed),
                    );

                    return includesPending && !includesCompleted;
                });

                let noOfItemsRemoved = 0;

                // Update or remove requests based on shouldRemove condition
                const updatedRequests = shouldRemove
                    ? requests.filter((req) => {
                          const isCompleted = !requestUrns.includes(readField('urn', req) || '');
                          if (isCompleted) {
                              noOfItemsRemoved++;
                          }
                          return isCompleted;
                      })
                    : requests.map((req) =>
                          requestUrns.includes(readField('urn', req) || '')
                              ? {
                                    ...req,
                                    status: ActionRequestStatus.Completed,
                                    result,
                                    resultNote,
                                    lastModified: {
                                        ...req.lastModified,
                                        actor: {
                                            ...currentUser,
                                            type: EntityType.CorpUser,
                                            editableInfo: req.lastModified?.actor?.editableInfo ?? null,
                                        },
                                    },
                                }
                              : req,
                      );

                return {
                    ...existingData,
                    actionRequests: updatedRequests,
                    total: total - noOfItemsRemoved,
                };
            },
        },
    });
};
