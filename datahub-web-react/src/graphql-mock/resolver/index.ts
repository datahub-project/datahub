import { getBrowseResultsResolver } from './getBrowseResultsResolver';
import { getSearchResultsResolver } from './getSearchResultsResolver';
import { getAutoCompleteAllResultsResolver } from './getAutoCompleteAllResultsResolver';
import { getAutoCompleteResultsResolver } from './getAutoCompleteResultsResolver';
import { getBrowsePathsResolver } from './getBrowsePathsResolver';
import { getDatasetResolver } from './getDatasetResolver';
import { getDashboardResolver } from './getDashboardResolver';
import { getChartResolver } from './getChartResolver';
import { getDataFlowResolver } from './getDataFlowResolver';
import { getDataJobResolver } from './getDataJobResolver';
import { getTagResolver } from './getTagResolver';
import { isAnalyticsEnabledResolver } from './isAnalyticsEnabledResolver';
import { updateDatasetResolver } from './updateDatasetResolver';
import { updateDashboardResolver } from './updateDashboardResolver';
import { updateChartResolver } from './updateChartResolver';
import { updateDataFlowResolver } from './updateDataFlowResolver';
import { updateDataJobResolver } from './updateDataJobResolver';
import { updateTagResolver } from './updateTagResolver';

const resolver = {
    ...getSearchResultsResolver,
    ...getBrowseResultsResolver,
    ...getAutoCompleteAllResultsResolver,
    ...getAutoCompleteResultsResolver,
    ...getBrowsePathsResolver,
    ...getDatasetResolver,
    ...getDashboardResolver,
    ...getChartResolver,
    ...getDataFlowResolver,
    ...getDataJobResolver,
    ...getTagResolver,
    ...isAnalyticsEnabledResolver,
    ...updateDatasetResolver,
    ...updateDashboardResolver,
    ...updateChartResolver,
    ...updateDataFlowResolver,
    ...updateDataJobResolver,
    ...updateTagResolver,
};

export const resolveRequest = (schema, request) => {
    const { operationName, variables } = JSON.parse(request.requestBody);
    return resolver[operationName] && resolver[operationName]({ schema, variables });
};
