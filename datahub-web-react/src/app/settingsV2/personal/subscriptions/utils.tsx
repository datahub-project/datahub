import EntityRegistry from '@app/entityV2/EntityRegistry';
import { SubscriptionFilterOptions } from '@app/settingsV2/personal/subscriptions/types';

import {
    CorpGroup,
    CorpUser,
    DataHubSubscription,
    EntityChangeType,
    NotificationSettings,
    NotificationSinkType,
    TeamsChannel,
} from '@types';

export const getEntityChangeTypeDisplayName = (entityChangeType: EntityChangeType): string => {
    const displayNames: Record<EntityChangeType, string> = {
        [EntityChangeType.Deprecated]: 'Deprecated',
        [EntityChangeType.DocumentationChange]: 'Documentation Change',
        [EntityChangeType.GlossaryTermAdded]: 'Glossary Term Added',
        [EntityChangeType.GlossaryTermProposed]: 'Glossary Term Proposed',
        [EntityChangeType.GlossaryTermRemoved]: 'Glossary Term Removed',
        [EntityChangeType.OwnerAdded]: 'Owner Added',
        [EntityChangeType.OwnerRemoved]: 'Owner Removed',
        [EntityChangeType.OperationColumnAdded]: 'Column Added',
        [EntityChangeType.OperationColumnRemoved]: 'Column Removed',
        [EntityChangeType.OperationColumnModified]: 'Column Modified',
        [EntityChangeType.OperationRowsInserted]: 'Rows Inserted',
        [EntityChangeType.OperationRowsUpdated]: 'Rows Updated',
        [EntityChangeType.OperationRowsRemoved]: 'Rows Removed',
        [EntityChangeType.AssertionPassed]: 'Assertion Passed',
        [EntityChangeType.AssertionFailed]: 'Assertion Failed',
        [EntityChangeType.AssertionError]: 'Assertion Error',
        [EntityChangeType.IncidentRaised]: 'Incident Raised',
        [EntityChangeType.IncidentResolved]: 'Incident Resolved',
        [EntityChangeType.TestPassed]: 'Test Passed',
        [EntityChangeType.TestFailed]: 'Test Failed',
        [EntityChangeType.Undeprecated]: 'Undeprecated',
        [EntityChangeType.IngestionSucceeded]: 'Ingestion Succeeded',
        [EntityChangeType.IngestionFailed]: 'Ingestion Failed',
        [EntityChangeType.TagAdded]: 'Tag Added',
        [EntityChangeType.TagProposed]: 'Tag Proposed',
        [EntityChangeType.TagRemoved]: 'Tag Removed',
    };
    return displayNames[entityChangeType] || entityChangeType;
};

export const getMergedNotificationSettingsForSubscription = (
    subscription: DataHubSubscription,
    ownedAndMemberGroups: CorpGroup[],
    actorUrn: string,
    actorNotificationSettings?: NotificationSettings,
): {
    email: string | undefined;
    slackChannels: string[] | undefined;
    slackUserHandle: string | undefined;
    teamsUserDisplayName: string | undefined;
    teamsChannels: TeamsChannel[] | undefined;
    sinkTypes: NotificationSinkType[] | undefined;
} => {
    const subscriptionNotificationSettings = subscription.notificationConfig?.notificationSettings;
    const subscriptionOwnerUrn = subscription.actor.urn;
    const subscriptionEmail = subscriptionNotificationSettings?.emailSettings?.email;
    const subscriptionSinkTypes = subscriptionNotificationSettings?.sinkTypes;
    const subscriptionSlackChannels = subscriptionNotificationSettings?.slackSettings?.channels;
    const subscriptionSlackUserHandle = subscriptionNotificationSettings?.slackSettings?.userHandle;
    const subscriptionTeamsUserDisplayName = subscriptionNotificationSettings?.teamsSettings?.user?.displayName;
    const subscriptionTeamsChannels = subscriptionNotificationSettings?.teamsSettings?.channels;
    if (subscriptionOwnerUrn === actorUrn && actorNotificationSettings) {
        return {
            email: subscriptionEmail || actorNotificationSettings?.emailSettings?.email,
            slackChannels: subscriptionSlackChannels || actorNotificationSettings?.slackSettings?.channels || [],
            slackUserHandle:
                subscriptionSlackUserHandle || actorNotificationSettings?.slackSettings?.userHandle || undefined,
            teamsUserDisplayName:
                subscriptionTeamsUserDisplayName ||
                actorNotificationSettings?.teamsSettings?.user?.displayName ||
                undefined,
            teamsChannels: subscriptionTeamsChannels || actorNotificationSettings?.teamsSettings?.channels || [],
            sinkTypes: subscriptionSinkTypes || actorNotificationSettings?.sinkTypes || [],
        };
    }
    const owningGroup = ownedAndMemberGroups.find((group) => group.urn === subscriptionOwnerUrn);
    if (owningGroup) {
        const owningGroupNotificationSettings = owningGroup.notificationSettings;
        return {
            email: subscriptionEmail || owningGroupNotificationSettings?.emailSettings?.email,
            slackChannels: subscriptionSlackChannels || owningGroupNotificationSettings?.slackSettings?.channels || [],
            slackUserHandle:
                subscriptionSlackUserHandle || owningGroupNotificationSettings?.slackSettings?.userHandle || undefined,
            teamsUserDisplayName:
                subscriptionTeamsUserDisplayName ||
                owningGroupNotificationSettings?.teamsSettings?.user?.displayName ||
                undefined,
            teamsChannels: subscriptionTeamsChannels || owningGroupNotificationSettings?.teamsSettings?.channels || [],
            sinkTypes: subscriptionSinkTypes || owningGroupNotificationSettings?.sinkTypes || [],
        };
    }
    return {
        email: subscriptionEmail,
        slackChannels: subscriptionSlackChannels || [],
        slackUserHandle: subscriptionSlackUserHandle || undefined,
        teamsUserDisplayName: subscriptionTeamsUserDisplayName || undefined,
        teamsChannels: subscriptionTeamsChannels || [],
        sinkTypes: subscriptionSinkTypes || [],
    };
};

/** Create filter option list as per the subscription data present
 * for example
 * entityType: [
 *   {
 *     name: "DATASET",
 *     category: 'entityType',
 *     count: 10,
 *     displayName: "Dataset"
 *   }
 * ]
 */
export const extractFilterOptionListFromSubscriptions = (
    subscriptions: DataHubSubscription[],
    entityRegistry: EntityRegistry,
    viewer: CorpUser,
    groupUrns: string[],
    recommendedFilterMaxCount = 10,
): SubscriptionFilterOptions => {
    const filterOptions: SubscriptionFilterOptions = {
        filterGroupOptions: {
            entity: [],
            owner: [],
            eventType: [],
        },
        recommendedFilters: [],
    };

    // Track which options we've seen to add remaining ones with 0 count later
    const seenEntityTypes = new Set<string>();
    const seenEventTypes = new Set<string>();

    // Single iteration through subscriptions to collect all filter options
    subscriptions.forEach((subscription: DataHubSubscription) => {
        // Process entity type
        const entityType = subscription.entity?.type;
        if (entityType) {
            seenEntityTypes.add(entityType);
            const existingOption = filterOptions.filterGroupOptions.entity.find((opt) => opt.name === entityType);
            if (existingOption) {
                existingOption.count++;
            } else {
                filterOptions.filterGroupOptions.entity.push({
                    name: entityType,
                    category: 'entity',
                    count: 1,
                    displayName: entityRegistry.getCollectionName(entityType),
                });
            }
        }

        // Process event types
        const eventTypes = subscription.entityChangeTypes || [];
        eventTypes.forEach((eventType) => {
            const { entityChangeType } = eventType;
            seenEventTypes.add(entityChangeType);
            const existingOption = filterOptions.filterGroupOptions.eventType.find(
                (opt) => opt.name === entityChangeType,
            );
            if (existingOption) {
                existingOption.count++;
            } else {
                filterOptions.filterGroupOptions.eventType.push({
                    name: entityChangeType,
                    category: 'eventType',
                    count: 1,
                    displayName: getEntityChangeTypeDisplayName(entityChangeType),
                });
            }
        });

        // Process owner (actor)
        const { actor } = subscription;
        let displayName: string | undefined;

        if (actor?.__typename === 'CorpUser') {
            if (actor.urn === viewer.urn) {
                displayName = 'Owned by me';
            } else {
                displayName = actor.editableProperties?.displayName || actor.properties?.displayName || undefined;
            }
        } else if (actor?.__typename === 'CorpGroup') {
            displayName = actor.properties?.displayName || actor.info?.displayName || undefined;
        }

        if (displayName) {
            const existingOption = filterOptions.filterGroupOptions.owner.find((opt) => opt.name === actor.urn);
            if (existingOption) {
                existingOption.count++;
            } else {
                filterOptions.filterGroupOptions.owner.push({
                    name: actor.urn,
                    category: 'owner',
                    count: 1,
                    displayName,
                });
            }
        }
    });

    // Add remaining event types with 0 count
    Object.values(EntityChangeType).forEach((type) => {
        if (!seenEventTypes.has(type)) {
            filterOptions.filterGroupOptions.eventType.push({
                name: type,
                category: 'eventType',
                count: 0,
                displayName: getEntityChangeTypeDisplayName(type),
            });
        }
    });

    // First recommended filter is "owned by me"
    let recommendedFilterCountRemaining = recommendedFilterMaxCount;
    const ownedByMeFilter = filterOptions.filterGroupOptions.owner.find((opt) => opt.name === viewer.urn);
    if (ownedByMeFilter) {
        filterOptions.recommendedFilters.push(ownedByMeFilter);
        recommendedFilterCountRemaining--;
    }
    // Next recommended filter is subscriptions owned by groups the view is a member of
    const groupFilters = filterOptions.filterGroupOptions.owner
        .filter((opt) => groupUrns.includes(opt.name))
        .slice(0, recommendedFilterCountRemaining);
    if (groupFilters.length > 0) {
        filterOptions.recommendedFilters.push(...groupFilters);
    }
    recommendedFilterCountRemaining -= groupFilters.length;
    // Then, add event types
    const eventTypeFilters = filterOptions.filterGroupOptions.eventType
        .sort((a, b) => b.count - a.count)
        .slice(0, recommendedFilterCountRemaining);
    if (eventTypeFilters.length > 0) {
        filterOptions.recommendedFilters.push(...eventTypeFilters);
    }

    return filterOptions;
};
