import { useEffect, useMemo, useState } from 'react';

import { useGlobalSettings } from '@app/context/GlobalSettingsContext';
import { useUserContext } from '@app/context/useUserContext';
import { useEntityContext } from '@app/entity/shared/EntityContext';
import { mapSummaryElement } from '@app/entityV2/summary/properties/utils';
import { filterOutNonExistentModulesFromTemplate } from '@app/homeV3/context/hooks/utils/moduleOperationsUtils';
import { getDefaultSummaryPageTemplate } from '@app/homeV3/context/hooks/utils/utils';
import { DEFAULT_TEMPLATE } from '@app/homeV3/modules/constants';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { PageTemplateFragment } from '@graphql/template.generated';
import { PageTemplateSurfaceType } from '@types';

export function useTemplateState(templateType: PageTemplateSurfaceType) {
    const entityRegistry = useEntityRegistryV2();
    const [areTemplatesInitialized, setAreTemplatesInitialized] = useState(false);
    const [personalTemplate, setPersonalTemplate] = useState<PageTemplateFragment | null>(null);
    const [globalTemplate, setGlobalTemplate] = useState<PageTemplateFragment | null>(null);

    const { entityType, entityData } = useEntityContext();
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

    // setting default and local templates for asset summary page
    useEffect(() => {
        if (templateType === PageTemplateSurfaceType.AssetSummary && !!entityData) {
            setGlobalTemplate(getDefaultSummaryPageTemplate(entityType));
            setPersonalTemplate(entityData?.settings?.assetSummary?.templates?.[0].template || null);
        }
    }, [areTemplatesInitialized, entityType, entityData, templateType]);

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
        () => template?.properties.assetSummary?.summaryElements?.map((el) => mapSummaryElement(el, entityRegistry)),
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
