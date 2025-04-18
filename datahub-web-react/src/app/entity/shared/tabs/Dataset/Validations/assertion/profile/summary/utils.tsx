import React from 'react';

import { Typography } from 'antd';
import { Tooltip } from '@components';
import { Maybe } from 'graphql/jsutils/Maybe';
import {
    AssertionInfo,
    AssertionType,
    CronSchedule,
    DatasetAssertionInfo,
    EntityType,
    FieldAssertionInfo,
    FreshnessAssertionInfo,
    SchemaAssertionInfo,
    VolumeAssertionInfo,
} from '../../../../../../../../../types.generated';
import { DatasetAssertionDescription } from '../../../DatasetAssertionDescription';
import { FreshnessAssertionDescription } from '../../../FreshnessAssertionDescription';
import { VolumeAssertionDescription } from '../../../VolumeAssertionDescription';
import { SqlAssertionDescription } from '../../../SqlAssertionDescription';
import { FieldAssertionDescription } from '../../../FieldAssertionDescription';
import { SchemaAssertionDescription } from '../../../SchemaAssertionDescription';
import { useEntityRegistry } from '../../../../../../../../useEntityRegistry';
import { useGetUserQuery } from '../../../../../../../../../graphql/user.generated';
import { ANTD_GRAY_V2 } from '../../../../../../constants';

/**
 * Returns a text element describing the given assertion
 * If the assertion has a user-defined assertion, it'll prioritize displaying that.
 * Else it'll infer a description.
 * @param assertionInfo
 * @returns {JSX.Element}
 */
const useBuildPrimaryLabel = (
    assertionInfo?: Maybe<AssertionInfo>,
    monitorSchedule?: Maybe<CronSchedule>,
): JSX.Element => {
    let primaryLabel = <Typography.Text>No description found</Typography.Text>;
    if (assertionInfo?.description) {
        primaryLabel = <Typography.Text>{assertionInfo.description}</Typography.Text>;
    } else {
        switch (assertionInfo?.type) {
            case AssertionType.Dataset:
                primaryLabel = (
                    <DatasetAssertionDescription
                        assertionInfo={assertionInfo.datasetAssertion as DatasetAssertionInfo}
                    />
                );
                break;
            case AssertionType.Freshness:
                primaryLabel = (
                    <FreshnessAssertionDescription
                        assertionInfo={assertionInfo.freshnessAssertion as FreshnessAssertionInfo}
                        monitorSchedule={monitorSchedule}
                    />
                );
                break;
            case AssertionType.Volume:
                primaryLabel = (
                    <VolumeAssertionDescription assertionInfo={assertionInfo.volumeAssertion as VolumeAssertionInfo} />
                );
                break;
            case AssertionType.Sql:
                primaryLabel = <SqlAssertionDescription assertionInfo={assertionInfo} />;
                break;
            case AssertionType.Field:
                primaryLabel = (
                    <FieldAssertionDescription assertionInfo={assertionInfo.fieldAssertion as FieldAssertionInfo} />
                );
                break;
            case AssertionType.DataSchema:
                primaryLabel = (
                    <SchemaAssertionDescription assertionInfo={assertionInfo.schemaAssertion as SchemaAssertionInfo} />
                );
                break;
            default:
                break;
        }
    }
    return primaryLabel;
};

/**
 * Builds secondary label component containing information about who created and last updated the assertion
 * Has a tooltip on hover with richer context
 * @param assertionInfo
 * @returns {JSX.Element} if sufficient data is present
 */
const useBuildSecondaryLabel = (assertionInfo?: Maybe<AssertionInfo>): JSX.Element | null => {
    const entityRegistry = useEntityRegistry();

    // 1. Fetching the most recent actor data.
    const creatorActorUrn = assertionInfo?.source?.created?.actor;
    const updatedActorUrn = assertionInfo?.lastUpdated?.actor;
    const { data: createdActor } = useGetUserQuery({
        variables: { urn: creatorActorUrn ?? '', groupsCount: 0 },
        fetchPolicy: 'cache-first',
        skip: !creatorActorUrn,
    });
    const { data: lastUpdatedActor } = useGetUserQuery({
        variables: { urn: updatedActorUrn ?? '', groupsCount: 0 },
        fetchPolicy: 'cache-first',
        skip: !updatedActorUrn,
    });

    // 2. Define the secondary label message and the richer tooltip messages
    let secondaryLabelMessage = '';
    let tooltipCreatedByMessage = '';
    let tooltipLastUpdatedByMessage = '';

    // 2.1 Creator info if available
    if (createdActor?.corpUser && entityRegistry.getDisplayName(EntityType.CorpUser, createdActor.corpUser)) {
        secondaryLabelMessage = `Created by ${entityRegistry.getDisplayName(
            EntityType.CorpUser,
            createdActor.corpUser,
        )}`;

        tooltipCreatedByMessage = `Created by: ${entityRegistry.getDisplayName(
            EntityType.CorpUser,
            createdActor.corpUser,
        )}${createdActor.corpUser?.info?.email ? ` (${createdActor.corpUser.info.email})` : ''} on ${
            assertionInfo?.source?.created?.time
                ? new Date(assertionInfo.source.created.time).toLocaleString()
                : 'an unknown time' // should never get here
        }.`;
    }
    // 2.2 Last updated info if available
    if (lastUpdatedActor?.corpUser && entityRegistry.getDisplayName(EntityType.CorpUser, lastUpdatedActor.corpUser)) {
        // Special handling if creator is last updater
        if (updatedActorUrn === creatorActorUrn) {
            const prefix =
                assertionInfo?.source?.created?.time === assertionInfo?.lastUpdated?.time
                    ? `Created by`
                    : `Last updated by`;
            secondaryLabelMessage = `${prefix} ${entityRegistry.getDisplayName(
                EntityType.CorpUser,
                lastUpdatedActor.corpUser,
            )}`;
        } else {
            secondaryLabelMessage = `Last updated by ${entityRegistry.getDisplayName(
                EntityType.CorpUser,
                lastUpdatedActor.corpUser,
            )}`;
        }

        // Show tooltip label for last updated if either the updater != creator OR if updatedTime != createdTime
        if (
            updatedActorUrn !== creatorActorUrn ||
            assertionInfo?.lastUpdated?.time !== assertionInfo?.source?.created?.time
        ) {
            tooltipLastUpdatedByMessage = `Last updated by ${entityRegistry.getDisplayName(
                EntityType.CorpUser,
                lastUpdatedActor.corpUser,
            )}${lastUpdatedActor.corpUser?.info?.email ? ` (${lastUpdatedActor.corpUser.info.email})` : ''} on ${
                assertionInfo?.lastUpdated?.time
                    ? new Date(assertionInfo.lastUpdated.time).toLocaleString()
                    : 'an unknown time' // should never get here
            }.`;
        }
    }

    // 3. Construct the secondary label component if sufficient data exists
    return secondaryLabelMessage ? (
        <Tooltip
            title={
                <>
                    {tooltipCreatedByMessage ? [tooltipCreatedByMessage, <br />] : null}
                    {tooltipLastUpdatedByMessage}
                </>
            }
        >
            <Typography.Text style={{ color: ANTD_GRAY_V2['6'], marginLeft: 12, fontSize: 12 }}>
                {secondaryLabelMessage}
            </Typography.Text>
        </Tooltip>
    ) : null;
};

export const useBuildAssertionDescriptionLabels = (
    assertionInfo?: Maybe<AssertionInfo>,
    monitorSchedule?: Maybe<CronSchedule>,
): {
    primaryLabel: JSX.Element;
    secondaryLabel: JSX.Element | null;
} => {
    // ------- Primary label with assertion description ------ //
    const primaryLabel = useBuildPrimaryLabel(assertionInfo, monitorSchedule);

    // ----------- Try displaying secondary label showing creator/updater context ------------ //
    const secondaryLabel = useBuildSecondaryLabel(assertionInfo);

    return {
        primaryLabel,
        secondaryLabel,
    };
};
