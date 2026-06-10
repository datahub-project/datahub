import { Modal, message } from 'antd';
import i18next from 'i18next';

import { useEntityContext } from '@app/entity/shared/EntityContext';
import { EntityCapabilityType } from '@app/entityV2/Entity';
import { useReloadableContext } from '@app/sharedV2/reloadableContext/hooks/useReloadableContext';
import { ReloadableKeyTypeNamespace } from '@app/sharedV2/reloadableContext/types';
import { getReloadableKeyType } from '@app/sharedV2/reloadableContext/utils';
import { useBatchSetDataProductMutation } from '@src/graphql/dataProduct.generated';

import { useBatchSetApplicationMutation } from '@graphql/application.generated';
import { useRemoveTermMutation, useUnsetDomainMutation } from '@graphql/mutations.generated';
import { BrowsePathV2, DataHubPageModuleType, EntityType, GlobalTags, Owner } from '@types';

export function getUniqueOwners(owners?: Owner[] | null) {
    const uniqueOwnerUrns = new Set();
    return owners?.filter((owner) => !uniqueOwnerUrns.has(owner.owner.urn) && uniqueOwnerUrns.add(owner.owner.urn));
}

export const entityHasCapability = (
    capabilities: Set<EntityCapabilityType>,
    capabilityToCheck: EntityCapabilityType,
): boolean => capabilities.has(capabilityToCheck);

export const getHighlightedTag = (tags?: GlobalTags) => {
    if (tags && tags.tags?.length) {
        if (tags?.tags[0].tag.properties) return tags?.tags[0]?.tag?.properties?.name;
        return tags?.tags[0]?.tag?.name;
    }
    return '';
};

export const isNullOrUndefined = (value: any) => {
    return value === null || value === undefined;
};

// TODO: Change Modals in this file
export function useRemoveDomainAssets(setShouldRefetchEmbeddedListSearch) {
    const { entityState, refetch, entityType } = useEntityContext();
    const [unsetDomainMutation] = useUnsetDomainMutation();
    const { reloadByKeyType } = useReloadableContext();

    const handleRemoveDomain = (urnToRemoveFrom) => {
        message.loading({ content: i18next.t('entity.preview:domain.removing'), duration: 2 });
        unsetDomainMutation({ variables: { entityUrn: urnToRemoveFrom } })
            .then(() => {
                setTimeout(() => {
                    setShouldRefetchEmbeddedListSearch(true);
                    entityState?.setShouldRefetchContents(true);
                    refetch();
                    message.success({ content: i18next.t('entity.preview:domain.removed'), duration: 2 });
                    // Reload modules
                    // Assets - to update assets in domain summary tab
                    reloadByKeyType([
                        getReloadableKeyType(ReloadableKeyTypeNamespace.MODULE, DataHubPageModuleType.Assets),
                    ]);
                    // DataProduct - to update data products module in domain summary tab
                    if (entityType === EntityType.DataProduct) {
                        reloadByKeyType([
                            getReloadableKeyType(ReloadableKeyTypeNamespace.MODULE, DataHubPageModuleType.DataProducts),
                        ]);
                    }
                }, 2000);
            })
            .catch((e: unknown) => {
                message.destroy();
                if (e instanceof Error) {
                    message.error({
                        content: i18next.t('entity.preview:domain.removeError', { error: e.message || '' }),
                        duration: 3,
                    });
                }
            });
    };

    const removeDomain = (urnToRemoveFrom) => {
        Modal.confirm({
            title: i18next.t('entity.preview:domain.removeConfirmTitle'),
            content: i18next.t('entity.preview:domain.removeConfirmContent'),
            onOk() {
                handleRemoveDomain(urnToRemoveFrom);
            },
            onCancel() {},
            okText: i18next.t('common.actions:yes'),
            maskClosable: true,
            closable: true,
        });
    };

    return { removeDomain };
}

export function useRemoveGlossaryTermAssets(setShouldRefetchEmbeddedListSearch) {
    const { reloadByKeyType } = useReloadableContext();
    const [removeTermMutation] = useRemoveTermMutation();

    const handleRemoveTerm = (previewData, termUrn) => {
        if (termUrn) {
            message.loading({ content: i18next.t('entity.preview:term.removing'), duration: 2 });
            removeTermMutation({
                variables: {
                    input: {
                        termUrn,
                        resourceUrn: previewData?.urn,
                    },
                },
            })
                .then(({ errors }) => {
                    if (!errors) {
                        setTimeout(() => {
                            setShouldRefetchEmbeddedListSearch(true);
                            message.success({ content: i18next.t('entity.preview:term.removed'), duration: 2 });
                            reloadByKeyType([
                                getReloadableKeyType(ReloadableKeyTypeNamespace.MODULE, DataHubPageModuleType.Assets),
                            ]);
                        }, 2000);
                    }
                })
                .catch((e) => {
                    message.destroy();
                    message.error({
                        content: i18next.t('entity.preview:term.removeError', { error: e.message || '' }),
                        duration: 3,
                    });
                });
        }
    };

    const removeTerm = (previewData, termUrn) => {
        Modal.confirm({
            title: i18next.t('entity.preview:term.removeConfirmTitle', { name: previewData.name }),
            content: i18next.t('entity.preview:term.removeConfirmContent', { name: previewData.name }),
            onOk() {
                handleRemoveTerm(previewData, termUrn);
            },
            onCancel() {},
            okText: i18next.t('common.actions:yes'),
            maskClosable: true,
            closable: true,
        });
    };

    return { removeTerm };
}

export function useRemoveDataProductAssets(setShouldRefetchEmbeddedListSearch) {
    const { reloadByKeyType } = useReloadableContext();
    const [batchSetDataProductMutation] = useBatchSetDataProductMutation();

    function handleDataProduct(urn) {
        batchSetDataProductMutation({ variables: { input: { resourceUrns: [urn] } } })
            .then(() => {
                setTimeout(() => {
                    setShouldRefetchEmbeddedListSearch(true);
                    message.success({ content: i18next.t('entity.preview:dataProduct.removed'), duration: 2 });
                    reloadByKeyType([
                        getReloadableKeyType(ReloadableKeyTypeNamespace.MODULE, DataHubPageModuleType.Assets),
                    ]);
                }, 2000);
            })
            .catch((e: unknown) => {
                message.destroy();
                if (e instanceof Error) {
                    message.error({
                        content: e.message || i18next.t('entity.preview:dataProduct.removeError'),
                        duration: 3,
                    });
                }
            });
    }

    const removeDataProduct = (urn) => {
        Modal.confirm({
            title: i18next.t('entity.preview:dataProduct.removeConfirmTitle'),
            content: i18next.t('entity.preview:dataProduct.removeConfirmContent'),
            onOk() {
                handleDataProduct(urn);
            },
            onCancel() {},
            okText: i18next.t('common.actions:yes'),
            maskClosable: true,
            closable: true,
        });
    };

    return { removeDataProduct };
}

export function useRemoveApplicationAssets(setShouldRefetchEmbeddedListSearch) {
    const [batchSetApplicationMutation] = useBatchSetApplicationMutation();

    function handleApplication(urn) {
        batchSetApplicationMutation({ variables: { input: { resourceUrns: [urn] } } })
            .then(() => {
                setTimeout(() => {
                    setShouldRefetchEmbeddedListSearch(true);
                    message.success({ content: i18next.t('entity.preview:application.removed'), duration: 2 });
                }, 2000);
            })
            .catch((e: unknown) => {
                message.destroy();
                if (e instanceof Error) {
                    message.error({
                        content: i18next.t('entity.preview:application.removeError', { error: e.message }),
                        duration: 3,
                    });
                }
            });
    }

    const removeApplication = (urn) => {
        Modal.confirm({
            title: i18next.t('entity.preview:application.removeConfirmTitle'),
            content: i18next.t('entity.preview:application.removeConfirmContent'),
            onOk() {
                handleApplication(urn);
            },
            onCancel() {},
            okText: i18next.t('common.actions:yes'),
            maskClosable: true,
            closable: true,
        });
    };

    return { removeApplication };
}

export const isDefaultBrowsePath = (browsePaths: BrowsePathV2) => {
    return browsePaths.path?.length === 1 && browsePaths?.path[0]?.name === 'Default';
};
