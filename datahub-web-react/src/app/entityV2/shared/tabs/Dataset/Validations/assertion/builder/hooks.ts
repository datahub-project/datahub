import { useEffect } from 'react';
import { useLocation, useHistory } from 'react-router';
import { message } from 'antd';
import { DatasetFreshnessSourceType } from '../../../../../../../../types.generated';
import { getFreshnessSourceOption } from './utils';
import { getQueryParams } from '../../assertionUtils';

type ChangeSourceOptionContext = {
    sourceType: DatasetFreshnessSourceType;
    updateSourceType: (value: string) => void;
};

// Custom hook to update the source type when a condition is met
export const useChangeSourceOptionIf = (
    condition: boolean,
    { sourceType, updateSourceType }: ChangeSourceOptionContext,
) => {
    useEffect(() => {
        if (condition) {
            const sourceOption = getFreshnessSourceOption(sourceType);
            updateSourceType(sourceOption.name);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [condition]);
};

export const useAssertionURNCopyLink = (urn: string) => {
    const onCopyLink = () => {
        const assertionUrn = urn;

        // Create a URL with the assertion_urn query parameter
        const currentUrl = new URL(window.location.href);

        // Add or update the assertion_urn query parameter
        currentUrl.searchParams.set('assertion_urn', encodeURIComponent(assertionUrn));

        // The updated URL with the new or modified query parameter
        const assertionUrl = currentUrl.href;

        // Copy the URL to the clipboard
        navigator.clipboard.writeText(assertionUrl).then(
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

/**
 * Hook to manage the details view of assertions based on URL query parameters.
 *
 * @param {Function} setFocusAssertionUrn - Function to set details of the viewing assertion and open detail Modal.
 * @returns {Object} Object containing the 'assertionUrnParam' from the URL.
 */
export const useOpenAssertionDetailModal = (setFocusAssertionUrn) => {
    const location = useLocation();
    const history = useHistory();
    const assertionUrnParam = getQueryParams('assertion_urn', location);

    useEffect(() => {
        if (assertionUrnParam) {
            const decodedAssertionUrn = decodeURIComponent(assertionUrnParam);

            setFocusAssertionUrn(decodedAssertionUrn);

            // Remove the query parameter from the URL
            const newUrlParams = new URLSearchParams(location.search);
            newUrlParams.delete('assertion_urn');
            const newUrl = `${location.pathname}?${newUrlParams.toString()}`;

            // Use React Router's history.replace to replace the current URL
            history.replace(newUrl);
        }
    }, [assertionUrnParam, setFocusAssertionUrn, location.search, location.pathname, history]);

    return { assertionUrnParam };
};

/**
 * Hook for managing the assertion_urn query parameter and expanding relevant row keys.
 * @param {Array} groups - Array of assertion groups.
 * @param {Function} setExpandedRowKeys - Function to set the expanded row keys.
 * @returns {Object} - Object with assertionUrnParam and other relevant data.
 */
export const useExpandRowBasedOnAssertionUrn = (groups, setExpandedRowKeys) => {
    const location = useLocation();
    const assertionUrnParam = getQueryParams('assertion_urn', location);

    useEffect(() => {
        if (assertionUrnParam) {
            const decodedAssertionUrn = decodeURIComponent(assertionUrnParam);

            // Find the row key to expand based on the assertion URN
            const rowKeyToExpand = groups.find((group) =>
                group.assertions.some((assertion) => assertion.urn === decodedAssertionUrn),
            )?.name;

            if (rowKeyToExpand) {
                setExpandedRowKeys([rowKeyToExpand]);
            }
        }
    }, [groups, setExpandedRowKeys, assertionUrnParam, location.search]);

    return { assertionUrnParam };
};
