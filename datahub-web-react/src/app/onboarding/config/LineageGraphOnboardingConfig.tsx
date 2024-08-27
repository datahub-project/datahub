import React from 'react';
import { Typography } from 'antd';
import { OnboardingStep } from '../OnboardingStep';

export const LINEAGE_GRAPH_INTRO_ID = 'lineage-graph-intro';
export const LINEAGE_GRAPH_TIME_FILTER_ID = 'lineage-graph-time-filter';

export const LineageGraphOnboardingConfig: OnboardingStep[] = [
    {
        id: LINEAGE_GRAPH_INTRO_ID,
        title: 'Lineage Graph',
        content: (
            <Typography.Paragraph>
                <p>
                    You can view the <strong>Lineage Graph</strong> for an entity on this page.
                </p>
                <p>
                    Data <strong>Lineage</strong> allows you to visualize and understand both the upstream dependencies
                    and downstream consumers of this entity.
                </p>
                <p>
                    Learn more about <strong>Lineage</strong>{' '}
                    <a
                        target="_blank"
                        rel="noreferrer noopener"
                        href="https://datahubproject.io/docs/generated/lineage/lineage-feature-guide/"
                    >
                        here.
                    </a>
                </p>
            </Typography.Paragraph>
        ),
    },
    {
        id: LINEAGE_GRAPH_TIME_FILTER_ID,
        selector: `#${LINEAGE_GRAPH_TIME_FILTER_ID}`,
        title: 'Filter Lineage Edges by Date',
        content: (
            <Typography.Paragraph>
                <p>
                    You can click which dates you would like to see lineage edges for on this graph. By default, the
                    graph will show edges observed in the last 14 days. Note that manual lineage edges and edges without
                    time information will always be shown.
                </p>
            </Typography.Paragraph>
        ),
    },
];
