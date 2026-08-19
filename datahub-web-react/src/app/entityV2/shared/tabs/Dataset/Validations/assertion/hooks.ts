import type { MutationHookOptions } from '@apollo/client';
import { message } from 'antd';
import { useEffect } from 'react';
import { useTranslation } from 'react-i18next';
import { useHistory, useLocation } from 'react-router';

import { isValidAssertionUrnFormat } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/assertionUrnUtils';
import { getQueryParams } from '@app/entityV2/shared/tabs/Dataset/Validations/assertionUtils';

import {
    DeleteAssertionMutation,
    DeleteAssertionMutationVariables,
    useDeleteAssertionMutation,
} from '@graphql/assertion.generated';

export const useDeleteAssertionMutationWithCache = (
    baseOptions?: MutationHookOptions<DeleteAssertionMutation, DeleteAssertionMutationVariables>,
) => {
    const [deleteAssertion, result] = useDeleteAssertionMutation({
        ...baseOptions,
        update(cache, response, options) {
            baseOptions?.update?.(cache, response, options);
            const assertionUrn = options.variables?.urn;
            if (response.data?.deleteAssertion && assertionUrn) {
                cache.evict({
                    id: cache.identify({
                        __typename: 'Assertion',
                        urn: assertionUrn,
                    }),
                });
                cache.gc();
            }
        },
    });

    return [deleteAssertion, result] as const;
};

export const copyTextToClipboard = async (text: string): Promise<void> => {
    if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(text);
        return;
    }

    const textarea = document.createElement('textarea');
    textarea.value = text;
    textarea.setAttribute('readonly', '');
    textarea.style.position = 'fixed';
    textarea.style.opacity = '0';
    document.body.appendChild(textarea);
    textarea.focus();
    textarea.select();

    try {
        if (!document.execCommand('copy')) {
            throw new Error('Browser rejected clipboard copy');
        }
    } finally {
        document.body.removeChild(textarea);
    }
};

export const useAssertionURNCopyLink = (urn: string) => {
    const { t } = useTranslation('entity.profile.validations');

    const onCopyLink = async () => {
        const assertionUrn = urn;

        // Create a URL with the assertion_urn query parameter
        const currentUrl = new URL(window.location.href);

        // Add or update the assertion_urn query parameter
        currentUrl.searchParams.set('assertion_urn', encodeURIComponent(assertionUrn));

        // The updated URL with the new or modified query parameter
        const assertionUrl = currentUrl.href;

        try {
            await copyTextToClipboard(assertionUrl);
            message.success(t('action.clipboardCopied'));
        } catch {
            message.error(t('action.clipboardFailed'));
        }
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
    const { t } = useTranslation('entity.profile.validations');
    const location = useLocation();
    const history = useHistory();
    const assertionUrnParam = getQueryParams('assertion_urn', location);

    useEffect(() => {
        if (assertionUrnParam) {
            const decodedAssertionUrn = decodeURIComponent(assertionUrnParam);

            if (!isValidAssertionUrnFormat(decodedAssertionUrn)) {
                message.error(t('action.malformedAssertionLink', { urn: decodedAssertionUrn }));
                return;
            }

            setFocusAssertionUrn(decodedAssertionUrn);

            // Remove the query parameter from the URL
            const newUrlParams = new URLSearchParams(location.search);
            newUrlParams.delete('assertion_urn');
            const newUrl = `${location.pathname}?${newUrlParams.toString()}`;

            // Use React Router's history.replace to replace the current URL
            history.replace(newUrl);
        }
    }, [assertionUrnParam, setFocusAssertionUrn, location.search, location.pathname, history, t]);

    return { assertionUrnParam };
};
