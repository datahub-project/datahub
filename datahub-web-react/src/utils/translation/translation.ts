export function translateDisplayNames(t: any, displayName: string | null | undefined): string {
    if (!displayName) return '';
    const displayNameFormatted = displayName
        .trim()
        .replaceAll(' ', '')
        .replaceAll(/[^a-zA-Z\s]/g, '')
        .toLowerCase();

    const FIELD_TO_DISPLAY_NAMES = {
        // common
        domains: t('common.domains'),
        domain: t('common.domain'),
        ownedby: t('common.owner'),
        type: t('common.type'),
        glossaryterm: t('common.glossaryTerms'),
        glossaryterms: t('common.glossaryTerms'),
        platform: t('common.platform'),
        container: t('common.container'),
        dataproduct: t('common.dataProduct'),
        tasks: t('common.tasks'),
        charts: t('common.charts'),
        database: t('common.database'),
        table: t('common.table'),
        schema: t('common.schema'),

        // home
        platforms: t('home.module.Platforms'),
        recentlyviewed: t('home.module.RecentlyViewedEntities'),
        recentlyedited: t('home.module.RecentlyEditedEntities'),
        mostpopular: t('home.module.HighUsageEntities'),
        toptags: t('home.module.TopTags'),
        topglossaryterms: t('home.module.TopTerms'),
        recentsearches: t('home.module.RecentSearches'),

        // result search list
        dataset: t('common.dataset'),
        datasets: t('common.datasets'),
        dashboard: t('common.dashboard'),
        dashboards: t('common.dashboards'),

        // profile page
        ownerof: t('common.ownerof'),
        groups: t('common.groups'),
        users: t('common.users'),

        // empty messages
        emptytitledomain: t('domain.emptyTitle'),
        emptydescriptiondomain: t('domain.emptyDescription'),
        emptytitleowner: t('owner.emptyTitle'),
        emptydescriptionowner: t('owner.emptyDescription'),
        emptytitletag: t('tags.emptyTitle'),
        emptydescriptiontag: t('tags.emptyDescription'),
        emptytitleterm: t('terms.emptyTitle'),
        emptydescriptionterm: t('terms.emptyDescription'),
        emptytitledataproduct: t('dataProduct.emptyTitle'),
        emptydescriptiondataproduct: t('dataProduct.emptyDescription'),
        emptytitledocs: t('documentation.emptyTitle'),
        emptydescriptiondocs: t('documentation.emptyDescription'),
        emptydocumentationtitle: t('documentation.emptyTitle'),
        emptydocumentationdescription: t('documentation.emptyDescription'),
        emptycontainstitle: t('relatedTermType.containNoTerms'),
        emptycontainsdescription: t('relatedTermType.containNoTermsDescription'),
        emptyinheritstitle: t('relatedTermType.emptyInheritedFrom'),
        emptyinheritsdescription: t('relatedTermType.emptyInheritedFromDescription'),
        emptycontainedbytitle: t('relatedTermType.emptyContainedBy'),
        emptycontainedbydescription: t('relatedTermType.emptyContainedByDescription'),
        emptyinheritedbytitle: t('relatedTermType.emptyInheritedBy'),
        emptyinheritedbydescription: t('relatedTermType.emptyInheritedByDescription'),

        // empty tabs
        emptyqueriestitle: t('queries.emptyTitle'),
        emptyqueriesdescription: t('queries.emptyDescription'),

        // incident types
        incidentoperational: t('incident.operational'),
        incidentcustom: t('incident.other'),

        // ownership type
        ownershipbusinessownername: t('ownerType.BUSINESS_OWNER.name'),
        ownershipbusinessownerdescription: t('ownerType.BUSINESS_OWNER.description'),
        ownershiptechnicalownername: t('ownerType.TECHNICAL_OWNER.name'),
        ownershiptechnicalownerdescription: t('ownerType.TECHNICAL_OWNER.description'),
        ownershipdatastewardname: t('ownerType.DATA_STEWARD.name'),
        ownershipdatastewarddescription: t('ownerType.DATA_STEWARD.description'),
        ownershipnonename: t('ownerType.NONE.name'),
        ownershipnonedescription: t('ownerType.NONE.description'),

        // related terms: glossary
        contains: t('relatedTermType.contains'),
        containedby: t('relatedTermType.containedBy'),
        inherits: t('relatedTermType.inherits'),
        inheritedby: t('relatedTermType.inheritedBy'),

        // analytics
        datalandscapesummary: t('analytics.dataLandscapeSummary'),
        usageanalytics: t('analytics.usageAnalytics'),
        datahubusageanalytics: t('analytics.usageAnalytics'),
        weeklyactiveusers: t('analytics.activeUsersWithParam', { param: t('analytics.byWeek') }),
        monthlyactiveusers: t('analytics.activeUsersWithParam', { param: t('analytics.byMonth') }),
        entitiesbydomain: t('analytics.entitiesByDomain'),
        assetsbyplatform: t('analytics.assetsByPlatform'),
        newusers: t('analytics.newUsers'),
        topusers: t('analytics.topUsers'),
        numberofsearches: t('analytics.numberOfSearches'),
        topvieweddataset: t('analytics.topViewedDatasets'),
        actionsbyentitytype: t('analytics.actionsByEntityType'),
        tabviewsbyentitytype: t('analytics.tabViewsByEntityType'),
        topsearchqueries: t('analytics.topSearchQueries'),

        entitiesperdomain: t('analytics.entitiesPerDomain'),
        entitiesperplatform: t('analytics.entitiesPerPlatform'),
        entitiesperterm: t('analytics.entitiesPerTerm'),
        searcheslastweek: t('analytics.searchesLastWeek'),
        sectionviewsacrossentitytypes: t('analytics.sectionViewsAcrossEntitytypes'),

        // onboarding
        businessglossary: t('onBoarding.businessGlossary.businessGlossaryIntroTitle'),
        businessglossarydescription: t('onBoarding.businessGlossary.businessGlossaryIntro_component'),
        glossarytermsdescription: t('onBoarding.businessGlossary.businessGlossaryCreateTerm_component'),
        glossarytermgroups: t('onBoarding.businessGlossary.businessGlossaryCreateTermGroupTitle'),

        glossarytermgroupsdescription: t('onBoarding.businessGlossary.businessGlossaryCreateTermGroup_component'),
        domainsdescription: t('onBoarding.domains.domainIntro_component'),
        createanewdomaindescription: t('onBoarding.domains.createDomains_component'),
        createanewdomain: t('onBoarding.domains.createDomainTitle'),

        entities_component: t('onBoarding.entityProfile.entities_component'),
        properties_component: t('onBoarding.entityProfile.properties_component'),
        documentation_component: t('onBoarding.entityProfile.documentation_component'),
        lineage_component: t('onBoarding.entityProfile.lineage_component'),
        schema_component: t('onBoarding.entityProfile.schema_component'),
        owners_component: t('onBoarding.entityProfile.owners_component'),
        tags_component: t('onBoarding.entityProfile.tags_component'),
        glossaryTerm_component: t('onBoarding.entityProfile.glossaryTerm_component'),
        domains_component: t('onBoarding.entityProfile.domains_component'),

        groupsdescription: t('onBoarding.groups.groupsIntro_component'),
        createanewgroupdescription: t('onBoarding.groups.createGroups_component'),
        createanewgroup: t('onBoarding.groups.createGroupsTitle'),

        createGroups_component: t('onBoarding.groups.createGroups_component'),
        ingestionCreate_component: t('onBoarding.ingestion.ingestionCreate_component'),
        ingestionRefreshSource_component: t('onBoarding.ingestion.ingestionRefreshSource_component'),
        introLineageGraph_component: t('onBoarding.lineageGraph.introLineageGraph_component'),
        timeFilter_component: t('onBoarding.lineageGraph.timeFilter_component'),
        rolesdescription: t('onBoarding.policies.introPolicies_component'),
        createPolicies_component: t('onBoarding.policies.createPolicies_component'),
        usersdescription: t('onBoarding.users.usersIntro_component'),
        usersInviteLink_component: t('onBoarding.users.usersInviteLink_component'),
        usersAssignRoleID_component: t('onBoarding.users.usersAssignRoleID_component'),
        createanewingestionsource: t('onBoarding.ingestion.ingestionCreateTitle'),
        createanewingestionsourcedescription: t('onBoarding.ingestion.ingestionCreate_component'),
        refreshingestionsources: t('onBoarding.ingestion.ingestionRefreshSourceTitle'),
        refreshingestionsourcesdescription: t('onBoarding.ingestion.ingestionRefreshSource_component'),
        configuringsinglesignonsso: t('onBoarding.users.usersSSOTitle'),
        configuringsinglesignonssodescription: t('onBoarding.users.usersSSO_component'),
        invitenewusers: t('onBoarding.users.usersInviteLinkTitle'),
        invitenewusersdescription: t('onBoarding.users.usersInviteLink_component'),
        assigningroles: t('onBoarding.users.usersAssignRoleIDTitle'),
        assigningrolesdescription: t('onBoarding.users.usersAssignRoleID_component'),
        funesdescription: t('onBoarding.roles.introID_component'),
        createanewpolicy: t('onBoarding.policies.createPoliciesTitle'),
        policies: t('common.policies'),
        policiesdescription: t('onBoarding.policies.introPolicies_component'),
        createanewpolicydescription: t('onBoarding.policies.createPolicies_component'),
        narrowyoursearch: t('onBoarding.search.searchResultFilterTitle'),
        narrowyoursearchdescription: t('onBoarding.search.searchResultFilterV2_component'),
        divedeeperwithadvancedfilters: t('onBoarding.search.searchResultAdvancedSearchTitle'),
        divedeeperwithadvancedfiltersdescription: t('onBoarding.search.searchResultAdvancedSearch_component'),
        exploreandrefineyoursearchbyplatform: t('onBoarding.search.searchResultBrowseTitle'),
        exploreandrefineyoursearchbyplatformdescription: t('onBoarding.search.searchResultBrowse_component'),
        explorebyplatform: t('onBoarding.homePage.platformsTitle'),
        explorebyplatformdescription: t('onBoarding.homePage.platforms_component'),
        explorarosmaispopularesdescription: t('onBoarding.homePage.mostPopular_component'),
        schematab: t('onBoarding.entityProfile.schemaTitle'),
        schematabdescription: t('onBoarding.entityProfile.schema_component'),
        documentationtab: t('onBoarding.entityProfile.documentationTitle'),
        documentationtabdescription: t('onBoarding.entityProfile.documentation_component'),
        lineagetab: t('onBoarding.entityProfile.lineageTitle'),
        lineagetabdescription: t('onBoarding.entityProfile.lineage_component'),
        propertiestab: t('onBoarding.entityProfile.propertiesTitle'),
        propertiestabdescription: t('onBoarding.entityProfile.properties_component'),
        ownersdescription: t('onBoarding.entityProfile.owners_component'),
        tagsdescription: t('onBoarding.entityProfile.tags_component'),
        domaindescription: t('onBoarding.entityProfile.domains_component'),
        ingestdata: t('onBoarding.homePage.ingestionTitle'),
        ingestdatadescription: t('onBoarding.homePage.ingestion_component'),
        findyourdata: t('onBoarding.homePage.searchBarTitle'),
        findyourdatadescription: t('onBoarding.homePage.searchBar_component'),
        welcometodatahub: t('onBoarding.homePage.welcomeTitle'),
        welcometodatahubdescription: `${t('onBoarding.homePage.welcomeParaph.intro_component')}<ul>
            <li>${t('onBoarding.homePage.welcomeParaph.listFeatures.quicklySearch_component')}</li>
            <li>${t('onBoarding.homePage.welcomeParaph.listFeatures.viewAndUnderstand_component')}</li>
            <li>${t('onBoarding.homePage.welcomeParaph.listFeatures.gainInsight_component')}</li>
            <li>${t('onBoarding.homePage.welcomeParaph.listFeatures.defineOwnership_component')}</li></ul>
            <p>${t('onBoarding.homePage.welcomeParaph.getStarted_component')}</p>
            ${t('onBoarding.homePage.welcomeParaph.pressKey_component')}`,

        // ingest
        lookmlGithubDeployKeyToolTipOne: t('ingest.lookml.lookmlGithubDeployKeyToolTipOne'),
        lookmlGithubDeployKeyToolTipTwo: t('ingest.lookml.lookmlGithubDeployKeyToolTipTwo'),
        thePreferredWayToObboardNewUsers: t('ingest.common.thePreferredWayToObboardNewUsers'),
        learnMoreAboutConfiguringSingleLogin: t('ingest.common.learnMoreAboutConfiguringSingleLogin'),
    };

    const entries = Object.entries(FIELD_TO_DISPLAY_NAMES);
    const entry = entries.find(([key]) => key === displayNameFormatted);

    if (entry) {
        return entry[1];
    }

    return displayName;
}
