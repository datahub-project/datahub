import { useEffect, useState } from 'react';
import { useLocation, useHistory } from 'react-router';
import { message } from 'antd';
import { getQueryParams } from '../../assertionUtils';

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

const OPEN_ASSERTION_BUILDER_QUERY_PARAM = 'open_assertion_builder';

/**
 * Hook to automatically open up assertion authoring on mount.
 *
 * @param {Function} onOpenAssertionBuilder - Function to open assertion builder.
 * @returns {Object} Object containing the 'open_assertion_builder' from the URL.
 */
export const useOpenAssertionBuilder = (onOpenAssertionBuilder: () => void) => {
    const location = useLocation();
    const history = useHistory();
    const openBuilderParam = getQueryParams(OPEN_ASSERTION_BUILDER_QUERY_PARAM, location);

    useEffect(() => {
        if (openBuilderParam) {
            const decodedOpenAssertionBuilder = decodeURIComponent(openBuilderParam);

            if (decodedOpenAssertionBuilder === 'true') {
                onOpenAssertionBuilder();
            }

            // Remove the query parameter from the URL
            const newUrlParams = new URLSearchParams(location.search);
            newUrlParams.delete(OPEN_ASSERTION_BUILDER_QUERY_PARAM);
            const newUrl = `${location.pathname}?${newUrlParams.toString()}`;

            // Use React Router's history.replace to replace the current URL
            history.replace(newUrl);
        }
    }, [openBuilderParam, onOpenAssertionBuilder, location.search, location.pathname, history]);

    return { openBuilderParam };
};

/**
 * Hook for managing the expanded row keys based on the `assertion_urn` query parameter.
 *
 * This hook ensures that relevant rows are expanded if an `assertion_urn` is present in the URL query parameters.
 * If no `assertion_urn` is provided, it expands all groups initially.
 *
 * @param {Array} groups - Array of assertion groups, where each group contains a list of assertions.
 * @returns {Object} - Object containing the expanded row keys and a function to update them.
 *  - expandedRowKeys: Array of currently expanded row keys.
 *  - setExpandedRowKeys: Function to manually set the expanded row keys.
 */
export const useExpandedRowKeys = (groups, { isGroupBy }: { isGroupBy: boolean }) => {
    const location = useLocation();
    const assertionUrnParam = new URLSearchParams(location.search).get('assertion_urn');
    const [expandedRowKeys, setExpandedRowKeys] = useState<string[]>([]);
    const [processed, setProcessed] = useState(false);

    useEffect(() => {
        if (isGroupBy) {
            if (assertionUrnParam) {
                const decodedAssertionUrn = decodeURIComponent(assertionUrnParam);

                // Find the row key to expand based on the assertion URN
                const rowKeyToExpand = groups.find((group) =>
                    group.assertions.some((assertion) => assertion.urn === decodedAssertionUrn),
                )?.name;

                if (rowKeyToExpand) {
                    setExpandedRowKeys((prevKeys) => [...prevKeys, rowKeyToExpand]);
                }

                setProcessed(true);
            } else if (!processed) {
                // If no assertion URN is present initially, set expandedRowKeys from groups
                const allGroupKeys = groups.map((group) => group.name);
                setExpandedRowKeys(allGroupKeys);
                setProcessed(true);
            }
        }
    }, [groups, assertionUrnParam, processed, isGroupBy]);

    return { expandedRowKeys, setExpandedRowKeys };
};
