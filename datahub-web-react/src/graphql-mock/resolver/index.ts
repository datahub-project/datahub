import { getAutoCompleteAllResultsResolver } from '@graphql-mock/resolver/getAutoCompleteAllResultsResolver';
import { getAutoCompleteResultsResolver } from '@graphql-mock/resolver/getAutoCompleteResultsResolver';
import { getBrowsePathsResolver } from '@graphql-mock/resolver/getBrowsePathsResolver';
import { getBrowseResultsResolver } from '@graphql-mock/resolver/getBrowseResultsResolver';
import { getChartResolver } from '@graphql-mock/resolver/getChartResolver';
import { getDashboardResolver } from '@graphql-mock/resolver/getDashboardResolver';
import { getDataFlowResolver } from '@graphql-mock/resolver/getDataFlowResolver';
import { getDataJobResolver } from '@graphql-mock/resolver/getDataJobResolver';
import { getDatasetResolver } from '@graphql-mock/resolver/getDatasetResolver';
import { getSearchResultsResolver } from '@graphql-mock/resolver/getSearchResultsResolver';
import { getTagResolver } from '@graphql-mock/resolver/getTagResolver';
import { isAnalyticsEnabledResolver } from '@graphql-mock/resolver/isAnalyticsEnabledResolver';
import { updateChartResolver } from '@graphql-mock/resolver/updateChartResolver';
import { updateDashboardResolver } from '@graphql-mock/resolver/updateDashboardResolver';
import { updateDataFlowResolver } from '@graphql-mock/resolver/updateDataFlowResolver';
import { updateDataJobResolver } from '@graphql-mock/resolver/updateDataJobResolver';
import { updateDatasetResolver } from '@graphql-mock/resolver/updateDatasetResolver';
import { updateTagResolver } from '@graphql-mock/resolver/updateTagResolver';

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
