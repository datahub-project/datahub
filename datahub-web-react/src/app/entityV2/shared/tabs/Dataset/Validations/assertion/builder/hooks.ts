import { message } from 'antd';
import { useEffect } from 'react';
import { useHistory, useLocation } from 'react-router';

import { getQueryParams } from '@app/entityV2/shared/tabs/Dataset/Validations/assertionUtils';

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
