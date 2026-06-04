import { Text } from '@components';
import React from 'react';
import { Trans } from 'react-i18next';

import { OnboardingStep } from '@app/onboarding/OnboardingStep';

export const LINEAGE_GRAPH_INTRO_ID = 'lineage-graph-intro';
export const LINEAGE_GRAPH_TIME_FILTER_ID = 'lineage-graph-time-filter';

export const LineageGraphOnboardingConfig: OnboardingStep[] = [
    {
        id: LINEAGE_GRAPH_INTRO_ID,
        title: <Trans i18nKey="onboarding:lineageGraph.introTitle" />,
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans i18nKey="onboarding:lineageGraph.introDescription" components={{ bold: <strong /> }} />
                </p>
                <p>
                    <Trans i18nKey="onboarding:lineageGraph.dataLineageDescription" components={{ bold: <strong /> }} />
                </p>
                <p>
                    <Trans
                        i18nKey="onboarding:lineageGraph.learnMore"
                        components={{
                            bold: <strong />,
                            anchor: (
                                // eslint-disable-next-line jsx-a11y/anchor-has-content, jsx-a11y/control-has-associated-label
                                <a
                                    target="_blank"
                                    rel="noreferrer noopener"
                                    href="https://docs.datahub.com/docs/features/feature-guides/lineage/"
                                />
                            ),
                        }}
                    />
                </p>
            </Text>
        ),
    },
    {
        id: LINEAGE_GRAPH_TIME_FILTER_ID,
        selector: `#${LINEAGE_GRAPH_TIME_FILTER_ID}`,
        title: <Trans i18nKey="onboarding:lineageGraph.timeFilterTitle" />,
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans i18nKey="onboarding:lineageGraph.timeFilterDescription" />
                </p>
            </Text>
        ),
    },
];
