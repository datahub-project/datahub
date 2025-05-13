import { GraphCard, LineChart } from '@components';
import dayjs from 'dayjs';
import React from 'react';

import NoPermission from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/NoPermission';
import GraphPopover from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/components/GraphPopover';
import MonthOverMonthPill from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/components/MonthOverMonthPill';
import MoreInfoModalContent from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/components/MoreInfoModalContent';
import { RowCountData } from '@app/entityV2/shared/useRowCountData';
import { formatNumberWithoutAbbreviation } from '@src/app/shared/formatNumber';
import { pluralize } from '@src/app/shared/textUtil';

const DEFAULT_GRAPH_NAME = 'Row Count';
const DEFAULT_GRAPH_HEIGHT = '290px';
const DEFAULT_GRAPH_LEFT_AXIS_PROPS = { hideZero: true };
const DEFAULT_GRAPH_MARGIN = { left: 50 };

type RowsProps = {
    /**
     * The height of the chart as a string in pixels e.g. '290px'
     */
    chartHeight?: string;
    /**
     * The data to display in the graph
     */
    data: RowCountData[];
    /**
     * Loading state for the graph
     */
    loading: boolean;
    /**
     * Permission to view dataset profile
     */
    canViewDatasetProfile: boolean;
    /**
     * Function to render additional controls on the chart e.g. AddAssertionButton, TimeRangeSelect
     */
    renderControls?: () => JSX.Element | null;
    /**
     * Whether to show the header
     */
    showHeader?: boolean;
    /**
     * Whether to show the empty (when there is no data) message header
     */
    showEmptyMessageHeader?: boolean;
    /**
     * The message to display when there is no data
     */
    emptyMessage?: string;
    /**
     * Content to display in the more info modal
     */
    moreInfoModalContent?: JSX.Element;
    /**
     * Optional data-testid property for the graph
     */
    dataTestId?: string;
};

export default function RowCountGraph({
    chartHeight = DEFAULT_GRAPH_HEIGHT,
    data,
    loading,
    canViewDatasetProfile,
    renderControls,
    showHeader = true,
    showEmptyMessageHeader = true,
    emptyMessage = 'No stats colllected for this asset at the moment.',
    moreInfoModalContent = <MoreInfoModalContent />,
    dataTestId,
}: RowsProps): JSX.Element {
    return (
        <GraphCard
            title={DEFAULT_GRAPH_NAME}
            showHeader={showHeader}
            showEmptyMessageHeader={showEmptyMessageHeader}
            emptyMessage={emptyMessage}
            isEmpty={data.length === 0 || !canViewDatasetProfile}
            emptyContent={!canViewDatasetProfile && <NoPermission statName="row count" />}
            loading={loading}
            graphHeight={chartHeight}
            renderControls={renderControls}
            dataTestId={dataTestId ? `${dataTestId}-card` : undefined}
            renderGraph={() => (
                <LineChart
                    data={data}
                    bottomAxisProps={{ tickFormat: (x) => dayjs(x).format('DD MMM') }}
                    leftAxisProps={DEFAULT_GRAPH_LEFT_AXIS_PROPS}
                    margin={DEFAULT_GRAPH_MARGIN}
                    dataTestId={dataTestId ? `${dataTestId}-chart` : undefined}
                    popoverRenderer={(datum: RowCountData) => (
                        <GraphPopover
                            header={dayjs(datum.x).format('dddd. MMM. D ’YY')}
                            value={`${formatNumberWithoutAbbreviation(datum.y)} ${pluralize(datum.y, 'Row')}`}
                            pills={<MonthOverMonthPill value={datum.mom} />}
                        />
                    )}
                />
            )}
            moreInfoModalContent={moreInfoModalContent}
        />
    );
}
