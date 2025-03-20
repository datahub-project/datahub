import React, { useCallback, useEffect, useMemo } from 'react';
import { message } from 'antd';
import { useHistory, useLocation } from 'react-router';

import { getTimeFromNow } from '@src/app/shared/time/timeUtils';
import { IncidentStagePill } from '@src/alchemy-components/components/IncidentStagePill';
import { IncidentPriorityLabel } from '@src/alchemy-components/components/IncidentPriorityLabel/IncidentPriorityLabel';
import { getCapitalizeWord } from '@src/alchemy-components/components/IncidentStagePill/utils';
import { AlignmentOptions } from '@src/alchemy-components/theme/config';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { CorpUser } from '@src/types.generated';
import { getQueryParams } from '../Dataset/Validations/assertionUtils';
import { getAssigneeNamesWithAvatarUrl, getLinkedAssetsCount } from './utils';
import { IncidentResolveButton } from './IncidentResolveButton';
import { IncidentAssigneeAvatarStack } from './IncidentAssigneeAvatarStack';
import { CategoryType } from './styledComponents';

export const useIncidentsTableColumns = () => {
    return useMemo(() => {
        const columns = [
            {
                title: 'Name',
                dataIndex: 'name',
                key: 'name',
                render: (record) =>
                    record.groupName ? (
                        <div>{record.groupName}</div>
                    ) : (
                        <IncidentPriorityLabel priority={record?.priority} title={record?.title} />
                    ),
                width: '25%',
                sorter: (a, b) => {
                    return a - b;
                },
            },
            {
                title: 'Stage',
                dataIndex: 'stage',
                key: 'stage',
                render: (record) =>
                    !record.groupName && (
                        <CategoryType data-testid="incident-stage">
                            <IncidentStagePill showLabel stage={record?.stage} />
                        </CategoryType>
                    ),
                width: '15%',
            },
            {
                title: 'Category',
                dataIndex: 'type',
                key: 'type',
                render: (record) =>
                    !record.groupName && (
                        <CategoryType data-testid="incident-category" title={getCapitalizeWord(String(record?.type))}>
                            {getCapitalizeWord(String(record?.type))}
                        </CategoryType>
                    ),
                sorter: (a, b) => {
                    return (b.type || '').localeCompare(a.type || '', undefined, { sensitivity: 'base' });
                },
                width: '12%',
            },
            {
                title: 'Opened ',
                dataIndex: 'created',
                key: 'created',
                render: (record) => {
                    return !record.groupName && <div>{getTimeFromNow(record.created)}</div>;
                },
                sorter: (a, b) => {
                    return a?.created - b?.created;
                },
                width: '12%',
            },
            {
                title: 'Assets',
                dataIndex: 'linkedAssets',
                tooltipTitle: 'Linked Assets',
                key: 'linkedAssets',
                width: '9%',
                render: (record) =>
                    !record.groupName && (
                        <div data-testid="incident-linked-assets">
                            {getLinkedAssetsCount(record?.linkedAssets?.length)}
                        </div>
                    ),
                sorter: (a, b) => {
                    return a.linkedAssets?.length - b.linkedAssets?.length;
                },
            },
            {
                title: 'Assignees',
                dataIndex: 'assignees',
                key: 'assignees',
                width: '12%',
                render: (record) =>
                    !record.groupName && (
                        <IncidentAssigneeAvatarStack assignees={getAssigneeNamesWithAvatarUrl(record?.assignees)} />
                    ),
                sorter: (a, b) => {
                    return a?.assignees?.length - b?.assignees?.length;
                },
            },
            {
                title: '',
                dataIndex: '',
                key: 'actions',
                width: '15%',
                render: (record) => {
                    return !record.groupName && <IncidentResolveButton incident={record} />;
                },
                alignment: 'right' as AlignmentOptions,
            },
        ];
        return columns;
    }, []);
};

export const useIncidentURNCopyLink = (Urn: string) => {
    const onCopyLink = () => {
        const assertionUrn = Urn;

        // Create a URL with the assertion_urn query parameter
        const currentUrl = new URL(window.location.href);

        // Add or update the assertion_urn query parameter
        currentUrl.searchParams.set('incident_urn', encodeURIComponent(assertionUrn));

        // The updated URL with the new or modified query parameter
        const incidentUrl = currentUrl.href;

        // Copy the URL to the clipboard
        navigator.clipboard.writeText(incidentUrl).then(
            () => {
                message.success('Link copied to clipboard!');
            },
            () => {
                message.error('Failed to copy link to clipboard.');
            },
        );
    };

    return onCopyLink;
};

export const getOnOpenAssertionLink = (Urn: string) => {
    return () => {
        if (!Urn) {
            return false;
        }
        const assertionUrn = Urn;

        // Create a URL with the assertion_urn query parameter
        const currentUrl = new URL(window.location.href);
        currentUrl.pathname = currentUrl.pathname.replace('Incidents', 'Quality/List');

        // Add or update the assertion_urn query parameter
        currentUrl.searchParams.set('assertion_urn', encodeURIComponent(assertionUrn));
        //  Delete is lineage mode from url
        currentUrl.searchParams.delete('is_lineage_mode');
        // The updated URL with the new or modified query parameter
        const assertionUrl = currentUrl.href;
        //  Replace current url with new one
        window.location.replace(assertionUrl);
        return true;
    };
};

/**
 * Hook to manage the details view of assertions based on URL query parameters.
 *
 * @param {Function} setFocusAssertionUrn - Function to set details of the viewing assertion and open detail Modal.
 * @returns {Object} Object containing the 'assertionUrnParam' from the URL.
 */
export const useOpenIncidentDetailModal = (setFocusIncidentUrn, updateIncidentData) => {
    const location = useLocation();
    const history = useHistory();
    const incidentUrnParam = getQueryParams('incident_urn', location);

    useEffect(() => {
        if (incidentUrnParam) {
            const decodedIncidentUrn = decodeURIComponent(incidentUrnParam);

            setFocusIncidentUrn(decodedIncidentUrn);
            updateIncidentData(decodedIncidentUrn);

            // Remove the query parameter from the URL
            const newUrlParams = new URLSearchParams(location.search);
            newUrlParams.delete('incident_urn');
            const newUrl = `${location.pathname}?${newUrlParams.toString()}`;

            // Use React Router's history.replace to replace the current URL
            history.replace(newUrl);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [incidentUrnParam, setFocusIncidentUrn, location.search, location.pathname, history]);

    return { incidentUrnParam };
};

export default function useGetUserName() {
    const entityRegistry = useEntityRegistryV2();

    return useCallback(
        (user: CorpUser) => {
            if (!user) return '';
            return entityRegistry.getDisplayName(user.type, user);
        },
        [entityRegistry],
    );
}
