import { toast } from '@components';
import { useCallback, useState } from 'react';
import { useTranslation } from 'react-i18next';

import analytics, { EntityActionType, EventType } from '@app/analytics';
import { handleBatchError } from '@app/entity/shared/utils';
import { useReloadableContext } from '@app/sharedV2/reloadableContext/hooks/useReloadableContext';
import { ReloadableKeyTypeNamespace } from '@app/sharedV2/reloadableContext/types';
import { getReloadableKeyType } from '@app/sharedV2/reloadableContext/utils';

import {
    useBatchAddTagsMutation,
    useBatchAddTermsMutation,
    useBatchRemoveTagsMutation,
    useBatchRemoveTermsMutation,
} from '@graphql/mutations.generated';
import { DataHubPageModuleType, EntityType, ResourceRefInput } from '@types';

export enum OperationType {
    ADD,
    REMOVE,
}

export const isAddOperation = (op?: OperationType) => op === OperationType.ADD || op === undefined;

interface RunMutationArgs {
    urns: string[];
    resources: ResourceRefInput[];
    type: EntityType.Tag | EntityType.GlossaryTerm;
    operationType: OperationType;
    onDone: () => void;
}

/**
 * Centralises the batch add/remove mutation flow used by both `AddTagsModal` and `AddTermsModal`.
 *
 * Returns a `runMutation` callback plus a `disableAction` flag that callers should reflect in their
 * submit button's disabled state. Analytics, toast notifications, the post-mutation module reload,
 * and the schema-field action-type discrimination all live here so the modal components stay
 * focused on UI concerns.
 */
export function useBatchTagTermMutation() {
    const { t } = useTranslation('shared.tags');
    const [disableAction, setDisableAction] = useState(false);
    const { reloadByKeyType } = useReloadableContext();

    const [batchAddTagsMutation] = useBatchAddTagsMutation();
    const [batchRemoveTagsMutation] = useBatchRemoveTagsMutation();
    const [batchAddTermsMutation] = useBatchAddTermsMutation();
    const [batchRemoveTermsMutation] = useBatchRemoveTermsMutation();

    const sendAnalytics = useCallback(
        (resources: ResourceRefInput[], type: EntityType.Tag | EntityType.GlossaryTerm) => {
            const isSchemaField = !!resources[0]?.subResource;
            let actionType: (typeof EntityActionType)[keyof typeof EntityActionType];
            if (isSchemaField) {
                actionType =
                    type === EntityType.Tag ? EntityActionType.UpdateSchemaTags : EntityActionType.UpdateSchemaTerms;
            } else {
                actionType = type === EntityType.Tag ? EntityActionType.UpdateTags : EntityActionType.UpdateTerms;
            }
            if (resources.length > 1) {
                analytics.event({
                    type: EventType.BatchEntityActionEvent,
                    actionType,
                    entityUrns: resources.map((r) => r.resourceUrn),
                });
            } else if (resources[0]) {
                analytics.event({
                    type: EventType.EntityActionEvent,
                    actionType,
                    entityType: type,
                    entityUrn: resources[0].resourceUrn,
                });
            }
        },
        [],
    );

    const runMutation = useCallback(
        ({ urns, resources, type, operationType, onDone }: RunMutationArgs) => {
            if (resources.length === 0) {
                onDone();
                return;
            }
            setDisableAction(true);

            const onError = (e: Error, fallback: string) => {
                const { content, duration } = handleBatchError(urns, e, { content: fallback, duration: 3 });
                toast.error(content, { duration });
            };
            const finish = () => {
                setDisableAction(false);
                onDone();
            };
            // Terms can show up in summary-tab modules; reload those after an add/remove so the page
            // doesn't show stale aggregations.
            const reloadAssetsModule = () => {
                reloadByKeyType(
                    [getReloadableKeyType(ReloadableKeyTypeNamespace.MODULE, DataHubPageModuleType.Assets)],
                    3000,
                );
            };

            if (type === EntityType.Tag) {
                if (isAddOperation(operationType)) {
                    batchAddTagsMutation({ variables: { input: { tagUrns: urns, resources } } })
                        .then(({ errors }) => {
                            if (!errors) {
                                toast.success(t('addTags.success'), { duration: 2 });
                                sendAnalytics(resources, type);
                            }
                        })
                        .catch((e) => onError(e, t('add.error', { error: e.message || '' })))
                        .finally(finish);
                } else {
                    batchRemoveTagsMutation({ variables: { input: { tagUrns: urns, resources } } })
                        .then(({ errors }) => {
                            if (!errors) toast.success(t('removeTags.success'), { duration: 2 });
                        })
                        .catch((e) => onError(e, t('remove.error', { error: e.message || '' })))
                        .finally(finish);
                }
                return;
            }

            if (isAddOperation(operationType)) {
                batchAddTermsMutation({ variables: { input: { termUrns: urns, resources } } })
                    .then(({ errors }) => {
                        if (!errors) {
                            toast.success(t('addTerms.success'), { duration: 2 });
                            sendAnalytics(resources, type);
                            reloadAssetsModule();
                        }
                    })
                    .catch((e) => onError(e, `Failed to add: \n ${e.message || ''}`))
                    .finally(finish);
            } else {
                batchRemoveTermsMutation({ variables: { input: { termUrns: urns, resources } } })
                    .then(({ errors }) => {
                        if (!errors) {
                            toast.success(t('removeTerms.success'), { duration: 2 });
                            reloadAssetsModule();
                        }
                    })
                    .catch((e) => onError(e, `Failed to remove: \n ${e.message || ''}`))
                    .finally(finish);
            }
        },
        [
            batchAddTagsMutation,
            batchAddTermsMutation,
            batchRemoveTagsMutation,
            batchRemoveTermsMutation,
            reloadByKeyType,
            sendAnalytics,
            t,
        ],
    );

    return { runMutation, disableAction };
}
