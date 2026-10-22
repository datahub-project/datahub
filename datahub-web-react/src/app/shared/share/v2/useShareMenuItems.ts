import { toast } from '@components';
import { Copy } from '@phosphor-icons/react/dist/csr/Copy';
import { EnvelopeSimple } from '@phosphor-icons/react/dist/csr/EnvelopeSimple';
import { LinkSimple } from '@phosphor-icons/react/dist/csr/LinkSimple';
import qs from 'query-string';
import { useCallback, useMemo } from 'react';
import { useTranslation } from 'react-i18next';

import { ItemType } from '@components/components/Menu/types';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';
import { resolveRuntimePath } from '@utils/runtimeBasePath';

function openMailClient(mailtoUrl: string): void {
    const anchor = document.createElement('a');
    anchor.href = mailtoUrl;
    anchor.target = '_blank';
    anchor.rel = 'noreferrer';
    anchor.click();
}

export function useShareMenuItems(): ItemType[] {
    const { t } = useTranslation('shared.share');
    const { t: tc } = useTranslation('common.actions');
    const { urn, entityType, entityData } = useEntityData();
    const entityRegistry = useEntityRegistryV2();

    const subType = entityData?.subTypes?.typeNames?.[0];
    const name = entityData?.name;
    const qualifiedName = entityData?.properties?.qualifiedName;
    const displayName = name || urn;
    const displayType = subType || entityRegistry.getEntityName(entityType) || entityType;

    const copyToClipboard = useCallback((value: string, confirmation: string) => {
        navigator.clipboard.writeText(value);
        toast.success(confirmation);
    }, []);

    return useMemo(() => {
        const items: ItemType[] = [];

        if (navigator.clipboard) {
            const shareUrl = `${window.location.origin}${resolveRuntimePath(
                entityRegistry.getEntityUrl(entityType, urn),
            )}/`;

            items.push({
                type: 'item',
                key: 'copyLink',
                title: tc('copyLink'),
                tooltip: t('copyLink.tooltip'),
                icon: LinkSimple,
                dataTestId: 'share-copy-link',
                onClick: () => copyToClipboard(shareUrl, t('copyLink.success')),
            });

            items.push({
                type: 'item',
                key: 'copyUrn',
                title: t('copyUrn.label'),
                tooltip: t('copyUrn.tooltip', { type: displayType }),
                icon: Copy,
                dataTestId: 'share-copy-urn',
                onClick: () => copyToClipboard(urn, t('copyUrn.success')),
            });

            items.push({
                type: 'item',
                key: 'copyName',
                title: t('copyName.label'),
                tooltip: t('copyName.tooltip', { type: displayType }),
                icon: Copy,
                dataTestId: 'share-copy-name',
                onClick: () => copyToClipboard(qualifiedName || displayName, t('copyName.success')),
            });
        }

        items.push({
            type: 'item',
            key: 'email',
            title: t('email.label'),
            tooltip: t('email.tooltip', { type: displayType }),
            icon: EnvelopeSimple,
            dataTestId: 'share-email',
            onClick: () => {
                openMailClient(
                    qs.stringifyUrl({
                        url: 'mailto:',
                        query: {
                            subject: t('email.subject', { displayName, displayType }),
                            body: t('email.body', {
                                displayType,
                                linkText: window.location.href,
                                urn,
                            }),
                        },
                    }),
                );
            },
        });

        return items;
    }, [t, tc, urn, entityType, entityRegistry, displayName, displayType, qualifiedName, copyToClipboard]);
}
