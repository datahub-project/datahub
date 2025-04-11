import { Modal, message } from 'antd';
import { useBatchSetDataProductMutation } from '@src/graphql/dataProduct.generated';
import { useRemoveTermMutation, useUnsetDomainMutation } from '../../graphql/mutations.generated';
import { BrowsePathV2, GlobalTags, Owner } from '../../types.generated';
import { EntityCapabilityType } from '../entityV2/Entity';
import { useEntityContext } from '../entity/shared/EntityContext';

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
    const { entityState, refetch } = useEntityContext();
    const [unsetDomainMutation] = useUnsetDomainMutation();

    const handleRemoveDomain = (urnToRemoveFrom) => {
        message.loading({ content: 'Removing Domain...', duration: 2 });
        unsetDomainMutation({ variables: { entityUrn: urnToRemoveFrom } })
            .then(() => {
                setTimeout(() => {
                    setShouldRefetchEmbeddedListSearch(true);
                    entityState?.setShouldRefetchContents(true);
                    refetch();
                    message.success({ content: 'Domain Removed!', duration: 2 });
                }, 2000);
            })
            .catch((e: unknown) => {
                message.destroy();
                if (e instanceof Error) {
                    message.error({ content: `Failed to remove domain: \n ${e.message || ''}`, duration: 3 });
                }
            });
    };

    const removeDomain = (urnToRemoveFrom) => {
        Modal.confirm({
            title: `Confirm Domain Removal`,
            content: `Are you sure you want to remove this domain?`,
            onOk() {
                handleRemoveDomain(urnToRemoveFrom);
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    return { removeDomain };
}

export function useRemoveGlossaryTermAssets(setShouldRefetchEmbeddedListSearch) {
    const [removeTermMutation] = useRemoveTermMutation();

    const handleRemoveTerm = (previewData, termUrn) => {
        if (termUrn) {
            message.loading({ content: 'Removing Term...', duration: 2 });
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
                            message.success({ content: 'Term Removed!', duration: 2 });
                        }, 2000);
                    }
                })
                .catch((e) => {
                    message.destroy();
                    message.error({ content: `Failed to remove Term: \n ${e.message || ''}`, duration: 3 });
                });
        }
    };

    const removeTerm = (previewData, termUrn) => {
        Modal.confirm({
            title: `Do you want to remove ${previewData.name} term?`,
            content: `Are you sure you want to remove the ${previewData.name} term?`,
            onOk() {
                handleRemoveTerm(previewData, termUrn);
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    return { removeTerm };
}

export function useRemoveDataProductAssets(setShouldRefetchEmbeddedListSearch) {
    const [batchSetDataProductMutation] = useBatchSetDataProductMutation();

    function handleDataProduct(urn) {
        batchSetDataProductMutation({ variables: { input: { resourceUrns: [urn] } } })
            .then(() => {
                setTimeout(() => {
                    setShouldRefetchEmbeddedListSearch(true);
                    message.success({ content: 'Removed Data Product.', duration: 2 });
                }, 2000);
            })
            .catch((e: unknown) => {
                message.destroy();
                if (e instanceof Error) {
                    message.error({
                        content: `Failed to remove data product. An unknown error occurred.`,
                        duration: 3,
                    });
                }
            });
    }

    const removeDataProduct = (urn) => {
        Modal.confirm({
            title: `Confirm Data Product Removal`,
            content: `Are you sure you want to remove this data product?`,
            onOk() {
                handleDataProduct(urn);
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    return { removeDataProduct };
}

export const isDefaultBrowsePath = (browsePaths: BrowsePathV2) => {
    return browsePaths.path?.length === 1 && browsePaths?.path[0]?.name === 'Default';
};
