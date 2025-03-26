import { GenericEntityProperties } from '@src/app/entity/shared/types';
import { useCallback, useMemo } from 'react';
import { ExternalLinkType } from '@src/app/analytics';
import { useAppConfig } from '@src/app/useAppConfig';
import { useIsSeparateSiblingsMode } from '../useIsSeparateSiblingsMode';
import { LinkAttributes } from './types';
import { getExternalUrlDisplayName } from '../utils';
import { getSiblings } from '../tabs/Dataset/Validations/acrylUtils';
import { sendClickExternalLinkAnalytics } from './utils';

const MAX_VISIBILE_ACTIONS = 2;

interface Action {
    displayName: string | undefined;
    url: string;
}

export default function usePlatrofmLinks(
    urn: string,
    genericEntityData: GenericEntityProperties | null,
    hideSiblingActions: boolean,
    suffix: string,
    className: string | undefined,
): LinkAttributes[] {
    const separateSiblings = useIsSeparateSiblingsMode();

    const appConfig = useAppConfig();
    const { showDefaultExternalLinks } = appConfig.config.featureFlags;

    const sendAnalytics = useCallback(() => {
        sendClickExternalLinkAnalytics(urn, genericEntityData?.type, ExternalLinkType.Default);
    }, [urn, genericEntityData]);

    return useMemo(() => {
        if (!showDefaultExternalLinks) return [];

        if (!genericEntityData) return [];

        const externalUrl = genericEntityData?.properties?.externalUrl;
        const parentPlatformName = getExternalUrlDisplayName(genericEntityData) + (suffix ?? '');
        const defaultAction = externalUrl ? [{ displayName: parentPlatformName || 'source', url: externalUrl }] : [];

        let visibleActions: Action[] = [...defaultAction];
        if (!(hideSiblingActions ?? separateSiblings)) {
            const siblings = getSiblings(genericEntityData);
            if (siblings && siblings.length) {
                const siblingActions: Action[] = siblings
                    .map((sibling) => {
                        if (sibling?.platform?.name && sibling?.properties?.externalUrl) {
                            return {
                                displayName: getExternalUrlDisplayName(sibling),
                                url: sibling.properties.externalUrl,
                            };
                        }
                        return null;
                    })
                    .filter((action): action is Action => action !== null);

                visibleActions = [...defaultAction, ...siblingActions].slice(0, MAX_VISIBILE_ACTIONS);
            }
        }

        return visibleActions.map((action) => ({
            url: action.url,
            label: action.displayName ? `View in ${action.displayName}` : action.url,
            onClick: sendAnalytics,
            className,
        }));
    }, [
        genericEntityData,
        hideSiblingActions,
        suffix,
        className,
        separateSiblings,
        sendAnalytics,
        showDefaultExternalLinks,
    ]);
}
