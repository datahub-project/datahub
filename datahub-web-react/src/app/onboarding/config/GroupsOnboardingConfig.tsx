import React from 'react';
import { Typography } from 'antd';
import { OnboardingStep } from '../OnboardingStep';

export const GROUPS_INTRO_ID = 'groups-intro';
export const GROUPS_CREATE_GROUP_ID = 'groups-create-group';

export const GroupsOnboardingConfig: OnboardingStep[] = [
    {
        id: GROUPS_INTRO_ID,
        title: 'Groups',
        content: (
            <Typography.Paragraph>
                <p>
                    Welcome to Datahub <strong>Groups</strong>!
                </p>
                <p>
                    <strong>Groups</strong> are collections of users which can be used to assign ownership to assets and
                    manage access.
                </p>
                <p>
                    <strong>Groups</strong> can be created natively within DataHub, or synced from your Identity
                    Provider.
                </p>
                <p>
                    Learn more about <strong>Groups</strong>{' '}
                    <a
                        target="_blank"
                        rel="noreferrer noopener"
                        href="https://datahubproject.io/docs/authorization/groups"
                    >
                        {' '}
                        here.
                    </a>
                </p>
            </Typography.Paragraph>
        ),
    },
    {
        id: GROUPS_CREATE_GROUP_ID,
        selector: `#${GROUPS_CREATE_GROUP_ID}`,
        title: 'Create a new Group',
        content: (
            <Typography.Paragraph>
                <p>
                    Click here to create a new <strong>Group</strong>.
                </p>
            </Typography.Paragraph>
        ),
    },
];
