import { toast } from '@components';
import i18next from 'i18next';
import { useState } from 'react';

import { useEntityContext } from '@app/entity/shared/EntityContext';
import { EntityCapabilityType } from '@app/entityV2/Entity';
import { useReloadableContext } from '@app/sharedV2/reloadableContext/hooks/useReloadableContext';
import { ReloadableKeyTypeNamespace } from '@app/sharedV2/reloadableContext/types';
import { getReloadableKeyType } from '@app/sharedV2/reloadableContext/utils';
import { useBatchSetDataProductMutation } from '@src/graphql/dataProduct.generated';

import { useBatchSetApplicationMutation } from '@graphql/application.generated';
import { useRemoveTermMutation, useUnsetDomainMutation } from '@graphql/mutations.generated';
import { BrowsePathV2, DataHubPageModuleType, EntityType, GlobalTags, Owner } from '@types';

export type RemoveConfirmState = {
    isOpen: boolean;
    handleClose: () => void;
    handleConfirm: () => void;
    modalTitle: string;
    modalText: string;
};

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

export function useRemoveDomainAssets(setShouldRefetchEmbeddedListSearch) {
    const { entityState, refetch, entityType } = useEntityContext();
    const [unsetDomainMutation] = useUnsetDomainMutation();
    const { reloadByKeyType } = useReloadableContext();
    const [urnToRemove, setUrnToRemove] = useState<string | null>(null);

    const handleRemoveDomain = (urnToRemoveFrom) => {
        toast.loading(i18next.t('entity.preview:domain.removing'), { duration: 2 });
        unsetDomainMutation({ variables: { entityUrn: urnToRemoveFrom } })
            .then(() => {
                setTimeout(() => {
                    setShouldRefetchEmbeddedListSearch(true);
                    entityState?.setShouldRefetchContents(true);
                    refetch();
                    toast.success(i18next.t('entity.preview:domain.removed'), { duration: 2 });
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
                toast.destroy();
                if (e instanceof Error) {
                    toast.error(i18next.t('entity.preview:domain.removeError', { error: e.message || '' }), {
                        duration: 3,
                    });
                }
            });
    };

    const removeDomain = (urnToRemoveFrom) => {
        setUrnToRemove(urnToRemoveFrom);
    };

    const confirmState: RemoveConfirmState = {
        isOpen: !!urnToRemove,
        handleClose: () => setUrnToRemove(null),
        handleConfirm: () => {
            if (urnToRemove) {
                handleRemoveDomain(urnToRemove);
            }
            setUrnToRemove(null);
        },
        modalTitle: i18next.t('entity.preview:domain.removeConfirmTitle'),
        modalText: i18next.t('entity.preview:domain.removeConfirmContent'),
    };

    return { removeDomain, confirmState };
}

export function useRemoveGlossaryTermAssets(setShouldRefetchEmbeddedListSearch) {
    const { reloadByKeyType } = useReloadableContext();
    const [removeTermMutation] = useRemoveTermMutation();
    const [pendingRemoval, setPendingRemoval] = useState<{ previewData: any; termUrn: string } | null>(null);

    const handleRemoveTerm = (previewData, termUrn) => {
        if (termUrn) {
            toast.loading(i18next.t('entity.preview:term.removing'), { duration: 2 });
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
                            toast.success(i18next.t('entity.preview:term.removed'), { duration: 2 });
                            reloadByKeyType([
                                getReloadableKeyType(ReloadableKeyTypeNamespace.MODULE, DataHubPageModuleType.Assets),
                            ]);
                        }, 2000);
                    }
                })
                .catch((e) => {
                    toast.destroy();
                    toast.error(i18next.t('entity.preview:term.removeError', { error: e.message || '' }), {
                        duration: 3,
                    });
                });
        }
    };

    const removeTerm = (previewData, termUrn) => {
        setPendingRemoval({ previewData, termUrn });
    };

    const confirmState: RemoveConfirmState = {
        isOpen: !!pendingRemoval,
        handleClose: () => setPendingRemoval(null),
        handleConfirm: () => {
            if (pendingRemoval) {
                handleRemoveTerm(pendingRemoval.previewData, pendingRemoval.termUrn);
            }
            setPendingRemoval(null);
        },
        modalTitle: i18next.t('entity.preview:term.removeConfirmTitle', {
            name: pendingRemoval?.previewData?.name,
        }),
        modalText: i18next.t('entity.preview:term.removeConfirmContent', {
            name: pendingRemoval?.previewData?.name,
        }),
    };

    return { removeTerm, confirmState };
}

export function useRemoveDataProductAssets(setShouldRefetchEmbeddedListSearch) {
    const { reloadByKeyType } = useReloadableContext();
    const [batchSetDataProductMutation] = useBatchSetDataProductMutation();
    const [urnToRemove, setUrnToRemove] = useState<string | null>(null);

    function handleDataProduct(urn) {
        batchSetDataProductMutation({ variables: { input: { resourceUrns: [urn] } } })
            .then(() => {
                setTimeout(() => {
                    setShouldRefetchEmbeddedListSearch(true);
                    toast.success(i18next.t('entity.preview:dataProduct.removed'), { duration: 2 });
                    reloadByKeyType([
                        getReloadableKeyType(ReloadableKeyTypeNamespace.MODULE, DataHubPageModuleType.Assets),
                    ]);
                }, 2000);
            })
            .catch((e: unknown) => {
                toast.destroy();
                if (e instanceof Error) {
                    toast.error(e.message || i18next.t('entity.preview:dataProduct.removeError'), { duration: 3 });
                }
            });
    }

    const removeDataProduct = (urn) => {
        setUrnToRemove(urn);
    };

    const confirmState: RemoveConfirmState = {
        isOpen: !!urnToRemove,
        handleClose: () => setUrnToRemove(null),
        handleConfirm: () => {
            if (urnToRemove) {
                handleDataProduct(urnToRemove);
            }
            setUrnToRemove(null);
        },
        modalTitle: i18next.t('entity.preview:dataProduct.removeConfirmTitle'),
        modalText: i18next.t('entity.preview:dataProduct.removeConfirmContent'),
    };

    return { removeDataProduct, confirmState };
}

export function useRemoveApplicationAssets(setShouldRefetchEmbeddedListSearch) {
    const [batchSetApplicationMutation] = useBatchSetApplicationMutation();
    const [urnToRemove, setUrnToRemove] = useState<string | null>(null);

    function handleApplication(urn) {
        batchSetApplicationMutation({ variables: { input: { resourceUrns: [urn] } } })
            .then(() => {
                setTimeout(() => {
                    setShouldRefetchEmbeddedListSearch(true);
                    toast.success(i18next.t('entity.preview:application.removed'), { duration: 2 });
                }, 2000);
            })
            .catch((e: unknown) => {
                toast.destroy();
                if (e instanceof Error) {
                    toast.error(i18next.t('entity.preview:application.removeError', { error: e.message }), {
                        duration: 3,
                    });
                }
            });
    }

    const removeApplication = (urn) => {
        setUrnToRemove(urn);
    };

    const confirmState: RemoveConfirmState = {
        isOpen: !!urnToRemove,
        handleClose: () => setUrnToRemove(null),
        handleConfirm: () => {
            if (urnToRemove) {
                handleApplication(urnToRemove);
            }
            setUrnToRemove(null);
        },
        modalTitle: i18next.t('entity.preview:application.removeConfirmTitle'),
        modalText: i18next.t('entity.preview:application.removeConfirmContent'),
    };

    return { removeApplication, confirmState };
}

export const isDefaultBrowsePath = (browsePaths: BrowsePathV2) => {
    return browsePaths.path?.length === 1 && browsePaths?.path[0]?.name === 'Default';
};
