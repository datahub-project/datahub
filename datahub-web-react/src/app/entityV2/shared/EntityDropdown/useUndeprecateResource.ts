import { toast } from '@components';
import { useTranslation } from 'react-i18next';

import analytics, { EventType } from '@app/analytics';

import { useBatchUpdateDeprecationMutation } from '@graphql/mutations.generated';
import { SubResourceType } from '@types';

type Props = {
    urn: string;
    /** Field path, when clearing the deprecation of a column rather than of the asset itself. */
    subResource?: string | null;
    subResourceType?: SubResourceType;
    refetch?: () => void | Promise<unknown>;
};

/**
 * Clears the deprecation of an asset or of one of its sub-resources. Goes through the batch mutation
 * because only its input carries a sub-resource — `updateDeprecation` can only address a whole urn.
 *
 * Resolves to whether the deprecation was actually cleared, so callers can keep a confirmation step
 * open on failure. Failures are reported to the user here, never rethrown.
 */
export function useUndeprecateResource({ urn, subResource, subResourceType, refetch }: Props) {
    const { t } = useTranslation('entity.shared.components');
    const [batchUpdateDeprecation] = useBatchUpdateDeprecationMutation();

    const resources = [{ resourceUrn: urn, subResource, subResourceType }];

    return async () => {
        try {
            const { errors } = await batchUpdateDeprecation({
                variables: {
                    input: {
                        resources,
                        deprecated: false,
                    },
                },
            });
            if (errors) {
                toast.error(t('deprecation.markUnDeprecatedError', { message: errors[0]?.message ?? '' }), {
                    duration: 3,
                });
                return false;
            }
            toast.success(t('deprecation.markedUnDeprecatedSuccess'), { duration: 2 });
            // The deprecation is already cleared, so a refresh that fails only leaves the view stale:
            // it must not escape as an unhandled rejection or report the clear as failed. Invoked
            // inside the chain so a callback that throws outright lands in the same handler.
            Promise.resolve()
                .then(() => refetch?.())
                .catch((err) => console.error('Failed to refetch after clearing a deprecation:', err));
            analytics.event({
                type: EventType.SetDeprecation,
                entityUrns: [urn],
                deprecated: false,
                resources: subResource ? resources : undefined,
            });
            return true;
        } catch (e: unknown) {
            toast.destroy();
            toast.error(t('deprecation.markUnDeprecatedError', { message: e instanceof Error ? e.message : '' }), {
                duration: 3,
            });
            return false;
        }
    };
}
