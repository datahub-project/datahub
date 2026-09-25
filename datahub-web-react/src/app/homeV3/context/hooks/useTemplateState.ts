import { useEffect, useMemo, useState } from 'react';

import { useGlobalSettings } from '@app/context/GlobalSettingsContext';
import { useUserContext } from '@app/context/useUserContext';
import { useEntityContext } from '@app/entity/shared/EntityContext';
import { mapSummaryElement } from '@app/entityV2/summary/properties/utils';
import { filterOutNonExistentModulesFromTemplate } from '@app/homeV3/context/hooks/utils/moduleOperationsUtils';
import {
    filterNonExistentStructuredProperties,
    getDefaultSummaryPageTemplate,
} from '@app/homeV3/context/hooks/utils/utils';
import { DEFAULT_TEMPLATE } from '@app/homeV3/modules/constants';
import usePrevious from '@app/shared/usePrevious';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { PageTemplateFragment } from '@graphql/template.generated';
import { PageTemplateSurfaceType } from '@types';

export function useTemplateState(templateType: PageTemplateSurfaceType) {
    const entityRegistry = useEntityRegistryV2();
    const [areTemplatesInitialized, setAreTemplatesInitialized] = useState(false);
    const [isAssetSummaryTemplateInitialized, setIsAssetSummaryTemplateInitialized] = useState(false);
    const [personalTemplate, setPersonalTemplate] = useState<PageTemplateFragment | null>(null);
    const [globalTemplate, setGlobalTemplate] = useState<PageTemplateFragment | null>(null);

    const { entityType, entityData, urn } = useEntityContext();
    const { settings, loaded: globalSettingsLoaded } = useGlobalSettings();
    const { user, loaded: userLoaded } = useUserContext();

    // setting default and local templates for home page
    useEffect(() => {
        if (
            globalSettingsLoaded &&
            userLoaded &&
            !areTemplatesInitialized &&
            templateType === PageTemplateSurfaceType.HomePage
        ) {
            setGlobalTemplate(
                filterOutNonExistentModulesFromTemplate(settings.globalHomePageSettings?.defaultTemplate) ||
                    DEFAULT_TEMPLATE,
            );
            setPersonalTemplate(
                filterOutNonExistentModulesFromTemplate(user?.settings?.homePage?.pageTemplate) || null,
            );
            setAreTemplatesInitialized(true);
        }
    }, [
        globalSettingsLoaded,
        userLoaded,
        areTemplatesInitialized,
        settings.globalHomePageSettings?.defaultTemplate,
        user?.settings?.homePage?.pageTemplate,
        templateType,
    ]);

    const prevUrn = usePrevious(urn);
    // reinitialization of templates for asset summary page when another entity is opened
    useEffect(() => {
        if (templateType === PageTemplateSurfaceType.AssetSummary && !!urn && !!prevUrn && prevUrn !== urn) {
            setGlobalTemplate(null);
            setPersonalTemplate(null);
            setIsAssetSummaryTemplateInitialized(false);
        }
    }, [templateType, prevUrn, urn]);

    // setting default and local templates for asset summary page
    useEffect(() => {
        if (
            !isAssetSummaryTemplateInitialized &&
            templateType === PageTemplateSurfaceType.AssetSummary &&
            !!entityData
        ) {
            setGlobalTemplate(getDefaultSummaryPageTemplate(entityType));
            setPersonalTemplate(entityData?.settings?.assetSummary?.templates?.[0].template || null);
            setIsAssetSummaryTemplateInitialized(true);
        }
    }, [isAssetSummaryTemplateInitialized, entityType, entityData, templateType]);

    const [isEditingGlobalTemplate, setIsEditingGlobalTemplate] = useState(false);

    // The current template is personal unless editing global or personal is missing
    const template = useMemo(
        () => (isEditingGlobalTemplate ? globalTemplate : personalTemplate || globalTemplate),
        [isEditingGlobalTemplate, personalTemplate, globalTemplate],
    );

    const setTemplate = (t: PageTemplateFragment | null) => {
        if (isEditingGlobalTemplate) {
            setGlobalTemplate(t);
        } else {
            setPersonalTemplate(t);
        }
    };

    const summaryElements = useMemo(
        () =>
            filterNonExistentStructuredProperties(template?.properties.assetSummary?.summaryElements || []).map((el) =>
                mapSummaryElement(el, entityRegistry),
            ),
        [template, entityRegistry],
    );

    return {
        personalTemplate,
        globalTemplate,
        template,
        isEditingGlobalTemplate,
        summaryElements,
        setIsEditingGlobalTemplate,
        setPersonalTemplate,
        setGlobalTemplate,
        setTemplate,
    };
}
