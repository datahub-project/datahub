import { orange } from '@ant-design/colors';
import { WarningFilled } from '@ant-design/icons';
import { Tooltip } from '@components';
import React from 'react';
import { useParams } from 'react-router';
import styled from 'styled-components';

import { generateQueryVariables } from '@app/entityV2/shared/embed/UpstreamHealth/utils';
import { decodeUrn } from '@app/entityV2/shared/utils';
import { useGetDefaultLineageStartTimeMillis } from '@app/lineage/utils/useGetLineageTimeParams';
import { HAS_ACTIVE_INCIDENTS_FILTER_NAME, HAS_FAILING_ASSERTIONS_FILTER_NAME } from '@app/search/utils/constants';
import { useUrlQueryParam } from '@app/shared/useUrlQueryParam';
import { useAppConfig } from '@app/useAppConfig';

import { useSearchAcrossLineageQuery } from '@graphql/search.generated';

// Do not update unless you update the reference to this ID in our Chrome extension code
const ICON_ID = 'embedded-datahub-health-icon';

// Default size for the warning icon in pixels
const DEFAULT_ICON_SIZE = 16;

const StyledWarning = styled(WarningFilled)<{ size: number }>`
    color: ${orange[5]};
    font-size: ${({ size }) => size}px;
`;

interface RouteParams {
    urn: string;
}

// Used by our Chrome Extension to show a warning health icon if there are unhealthy upstreams
export default function EmbeddedHealthIcon() {
    const { urn: encodedUrn } = useParams<RouteParams>();
    const { value: showTooltipParam } = useUrlQueryParam('show-tooltip', 'false');
    const { value: sizeParam } = useUrlQueryParam('size', DEFAULT_ICON_SIZE.toString());
    const showTooltip = showTooltipParam === 'true';
    const iconSize = parseInt(sizeParam || DEFAULT_ICON_SIZE.toString(), 10);
    const urn = decodeUrn(encodedUrn);
    const appConfig = useAppConfig();
    const lineageEnabled: boolean = appConfig?.config?.chromeExtensionConfig?.lineageEnabled || false;
    const startTimeMillis = useGetDefaultLineageStartTimeMillis();

    const { data: incidentsData } = useSearchAcrossLineageQuery(
        generateQueryVariables({
            urn,
            startTimeMillis,
            filterField: HAS_ACTIVE_INCIDENTS_FILTER_NAME,
            start: 0,
            includeAssertions: false,
            includeIncidents: true,
            skip: !lineageEnabled,
            count: 0,
        }),
    );

    const { data: assertionsData } = useSearchAcrossLineageQuery(
        generateQueryVariables({
            urn,
            startTimeMillis,
            filterField: HAS_FAILING_ASSERTIONS_FILTER_NAME,
            start: 0,
            includeAssertions: true,
            includeIncidents: false,
            skip: !lineageEnabled,
            count: 0,
        }),
    );

    if (incidentsData?.searchAcrossLineage?.total || assertionsData?.searchAcrossLineage?.total) {
        const warningIcon = <StyledWarning id={ICON_ID} size={iconSize} />;

        if (showTooltip) {
            return (
                <Tooltip title="Some upstream entities are unhealthy, which may impact this entity. Expand the DataHub browser extension's side panel for more details.">
                    {warningIcon}
                </Tooltip>
            );
        }

        return warningIcon;
    }

    return null;
}
