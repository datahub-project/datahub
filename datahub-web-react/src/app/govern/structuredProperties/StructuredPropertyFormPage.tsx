import { ActionsBar, Breadcrumb, Button, Loader, PageTitle, toast } from '@components';
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Redirect, useHistory, useLocation, useParams } from 'react-router';
import styled from 'styled-components';

import StructuredPropsForm from '@app/govern/structuredProperties/StructuredPropsForm';
import { PageContainer } from '@app/govern/structuredProperties/styledComponents';
import {
    AllowedValueRow,
    PropValueField,
    StructuredProp,
    StructuredPropertyFormErrors,
    createAllowedValueRow,
    getDisplayName,
    getNewAllowedPlatforms,
    getNewAllowedTypes,
    getNewEntityTypes,
    getStringOrNumberValueField,
    getValueType,
    hasFormErrors,
    haveAllowedValuesChanged,
    replaceAssetBadge,
    toAllowedValueInputs,
    validateStructuredProperty,
    valueTypes,
} from '@app/govern/structuredProperties/utils';
import {
    DiscardUnsavedChangesConfirmationProvider,
    useDiscardUnsavedChangesConfirmationContext,
} from '@app/sharedV2/confirmation/DiscardUnsavedChangesConfirmationContext';
import { useReloadableContext } from '@app/sharedV2/reloadableContext/hooks/useReloadableContext';
import { ReloadableKeyTypeNamespace } from '@app/sharedV2/reloadableContext/types';
import { getReloadableKeyType } from '@app/sharedV2/reloadableContext/utils';
import analytics, { EventType } from '@src/app/analytics';
import { useUserContext } from '@src/app/context/useUserContext';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import { PageRoutes } from '@src/conf/Global';
import { useGetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import {
    useCreateStructuredPropertyMutation,
    useGetStructuredPropertyQuery,
    useUpdateStructuredPropertyMutation,
} from '@src/graphql/structuredProperties.generated';
import {
    AllowedValueInput,
    EntityType,
    PropertyCardinality,
    StructuredPropertyEntity,
    UpdateStructuredPropertyInput,
} from '@src/types.generated';

// The floating ActionsBar is positioned against this container, so the scrolling happens on an
// inner element instead — otherwise the bar would scroll away with the form.
const FormPageContainer = styled(PageContainer)`
    position: relative;
    overflow: hidden;
    padding: 0;
    gap: 0;
`;

const ScrollArea = styled.div`
    flex: 1;
    min-height: 0;
    overflow-y: auto;
`;

const CenteredColumn = styled.div`
    box-sizing: border-box;
    width: 100%;
    max-width: 880px;
    margin: 0 auto;
    /* Bottom padding clears the floating actions bar so the last field stays reachable. */
    padding: 24px 64px 104px;

    @media only screen and (max-width: 900px) {
        padding: 24px 32px 104px;
    }
`;

const TitleWrapper = styled.div`
    margin: 16px 0 24px;
`;

type RouteParams = {
    urn?: string;
};

type LocationState = {
    readOnly?: boolean;
};

const SHOW_ASSET_BADGE_FILTER = 'showAsAssetBadge';

// The dirty flag lives in the page (it drives `enableRedirectHandling`), but the context that
// consumes it is only available below the provider, so this bridges the two.
const DirtyStateSync = ({ isDirty }: { isDirty: boolean }) => {
    const { setIsDirty } = useDiscardUnsavedChangesConfirmationContext();

    useEffect(() => {
        setIsDirty(isDirty);
    }, [isDirty, setIsDirty]);

    return null;
};

type SavedPropertyEvent =
    | { type: EventType.CreateStructuredPropertyEvent }
    | { type: EventType.EditStructuredPropertyEvent; propertyUrn: string };

export default function StructuredPropertyFormPage() {
    const { t } = useTranslation('governance.structured-properties');
    const { t: tc } = useTranslation('common.actions');
    const history = useHistory();
    const location = useLocation<LocationState>();
    // The URN is percent-encoded into the path, and react-router v5 hands params back undecoded.
    const { urn: urnParam } = useParams<RouteParams>();
    const urn = urnParam && decodeURIComponent(urnParam);
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const me = useUserContext();
    const canEditProps = me.platformPrivileges?.manageStructuredProperties;
    const isReadOnly = !canEditProps || !!location.state?.readOnly;
    const isEditMode = !!urn;

    const { data, loading } = useGetStructuredPropertyQuery({
        variables: { urn: urn ?? '' },
        skip: !urn,
    });
    const selectedProperty = data?.entity as StructuredPropertyEntity | undefined;
    const { data: badgeData } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                types: [EntityType.StructuredProperty],
                query: '*',
                start: 0,
                count: 1,
                searchFlags: { skipCache: true },
                orFilters: [{ and: [{ field: SHOW_ASSET_BADGE_FILTER, values: ['true'] }] }],
            },
        },
        fetchPolicy: 'network-only',
    });
    const badgeProperty = badgeData?.searchAcrossEntities?.searchResults?.[0]?.entity as
        | StructuredPropertyEntity
        | undefined;

    const [createStructuredProperty] = useCreateStructuredPropertyMutation();
    const [updateStructuredProperty] = useUpdateStructuredPropertyMutation();
    const [cardinality, setCardinality] = useState<PropertyCardinality>(PropertyCardinality.Single);
    const [formValues, setFormValues] = useState<StructuredProp>();
    const [selectedValueType, setSelectedValueType] = useState('');
    const [savedAllowedValues, setSavedAllowedValues] = useState<AllowedValueRow[]>();
    const [allowedValueRows, setAllowedValueRows] = useState<AllowedValueRow[]>([]);
    const [errors, setErrors] = useState<StructuredPropertyFormErrors>({});
    const [isSubmitting, setIsSubmitting] = useState(false);
    const [isSaveComplete, setIsSaveComplete] = useState(false);
    const [isDirty, setIsDirty] = useState(false);
    const [valueField, setValueField] = useState<PropValueField>('stringValue');
    const hydratedAllowedValues = useRef<{ propertyUrn?: string; valueField?: PropValueField }>({});

    const { reloadByKeyType } = useReloadableContext();

    const markDirty = useCallback(() => setIsDirty(true), []);

    const setFieldValue = useCallback(
        (field: keyof StructuredProp, value: StructuredProp[keyof StructuredProp]) => {
            setFormValues((prev) => ({ ...prev, [field]: value }));
            if (
                field === 'displayName' ||
                field === 'valueType' ||
                field === 'entityTypes' ||
                field === 'qualifiedName'
            ) {
                setErrors((current) => ({ ...current, [field]: undefined }));
            }
            markDirty();
        },
        [markDirty],
    );

    const addAllowedValueRow = useCallback(() => {
        setAllowedValueRows((rows) => [...rows, createAllowedValueRow()]);
        markDirty();
    }, [markDirty]);

    const updateAllowedValueRow = useCallback(
        (rowId: string, patch: Partial<AllowedValueRow>) => {
            setAllowedValueRows((rows) => rows.map((row) => (row.rowId === rowId ? { ...row, ...patch } : row)));
            setErrors((current) => ({ ...current, allowedValues: undefined }));
            markDirty();
        },
        [markDirty],
    );

    const removeAllowedValueRow = useCallback(
        (rowId: string) => {
            setAllowedValueRows((rows) => rows.filter((row) => row.rowId !== rowId));
            markDirty();
        },
        [markDirty],
    );

    const moveAllowedValueRow = useCallback(
        (from: number, to: number) => {
            setAllowedValueRows((rows) => {
                const reordered = [...rows];
                const [moved] = reordered.splice(from, 1);
                reordered.splice(to, 0, moved);
                return reordered;
            });
            markDirty();
        },
        [markDirty],
    );

    const returnToList = () => {
        history.push(PageRoutes.STRUCTURED_PROPERTIES);
    };

    const showErrorMessage = () => {
        toast.error(t(isEditMode ? 'updateError' : 'createError'), { duration: 3 });
    };

    const showSuccessMessage = () => {
        toast.success(t(isEditMode ? 'updateSuccess' : 'createSuccess'), { duration: 3 });
    };

    const trackSavedProperty = (event: SavedPropertyEvent, allowedValues: AllowedValueInput[]) => {
        analytics.event({
            ...event,
            propertyType: valueTypes.find((type) => type.value === formValues?.valueType)?.urn || '',
            appliesTo: formValues?.entityTypes ?? [],
            qualifiedName: formValues?.qualifiedName,
            allowedAssetTypes: formValues?.typeQualifier?.allowedTypes,
            allowedValues,
            cardinality,
            isHidden: formValues?.settings?.isHidden ?? false,
            showInSearchFilters: formValues?.settings?.showInSearchFilters ?? false,
            showAsAssetBadge: formValues?.settings?.showAsAssetBadge ?? false,
            showInAssetSummary: formValues?.settings?.showInAssetSummary ?? false,
            hideInAssetSummaryWhenEmpty: formValues?.settings?.hideInAssetSummaryWhenEmpty ?? false,
            showInColumnsTable: formValues?.settings?.showInColumnsTable ?? false,
        });
    };

    const finishSave = (badgeReplaceFailed = false) => {
        setIsDirty(false);
        setIsSaveComplete(true);
        if (badgeReplaceFailed) {
            toast.warning(t('badgeReplaceError'), { duration: 4 });
        } else {
            showSuccessMessage();
        }
        reloadByKeyType([
            getReloadableKeyType(ReloadableKeyTypeNamespace.STRUCTURED_PROPERTY, 'EntitySummaryTabSidebar'),
        ]);
    };

    useEffect(() => {
        if (isSaveComplete) {
            history.push(PageRoutes.STRUCTURED_PROPERTIES);
        }
    }, [history, isSaveComplete]);

    useEffect(() => {
        // A pending mutation cannot be safely abandoned: its completion could otherwise redirect
        // from whichever page the user navigated to. Block in-app transitions until it settles.
        // Releasing on `isSaveComplete` rather than waiting for `isSubmitting` to clear matters:
        // those land in separate commits, and effect cleanups all run before any effect body, so
        // this unblocks ahead of the redirect above instead of cancelling it.
        if (!isSubmitting || isSaveComplete) return undefined;
        return history.block(() => false);
    }, [history, isSubmitting, isSaveComplete]);

    const handleSubmit = async () => {
        const validationErrors = validateStructuredProperty(formValues, allowedValueRows);
        setErrors(validationErrors);
        if (hasFormErrors(validationErrors)) return;

        // Every property needs a value type, and an unrepresentable one (e.g. a value type and
        // cardinality with no matching option) can only be reported against that field.
        const valueTypeUrn = valueTypes.find((type) => type.value === formValues?.valueType)?.urn;
        if (!valueTypeUrn) {
            setErrors({ ...validationErrors, valueType: t('create.propertyTypeError') });
            return;
        }

        const allowedValues = toAllowedValueInputs(allowedValueRows);
        const settings = {
            isHidden: formValues?.settings?.isHidden ?? false,
            showInSearchFilters: formValues?.settings?.showInSearchFilters ?? false,
            showAsAssetBadge: formValues?.settings?.showAsAssetBadge ?? false,
            showInAssetSummary: formValues?.settings?.showInAssetSummary ?? false,
            hideInAssetSummaryWhenEmpty: formValues?.settings?.hideInAssetSummaryWhenEmpty ?? false,
            showInColumnsTable: formValues?.settings?.showInColumnsTable ?? false,
        };

        try {
            setIsSubmitting(true);
            let savedPropertyUrn: string | undefined;

            if (isEditMode && selectedProperty) {
                const allowedValuesChanged = haveAllowedValuesChanged(savedAllowedValues, allowedValueRows);
                const input: UpdateStructuredPropertyInput = {
                    urn: selectedProperty.urn,
                    displayName: formValues?.displayName,
                    description: formValues?.description,
                    typeQualifier: {
                        newAllowedTypes: getNewAllowedTypes(selectedProperty, formValues ?? {}),
                    },
                    newEntityTypes: getNewEntityTypes(selectedProperty, formValues ?? {}),
                    newAllowedPlatforms: getNewAllowedPlatforms(selectedProperty, formValues ?? {}),
                    allowedValues: allowedValuesChanged ? allowedValues : undefined,
                    setCardinalityAsMultiple: cardinality === PropertyCardinality.Multiple,
                    settings,
                };

                await updateStructuredProperty({ variables: { input } });
                savedPropertyUrn = selectedProperty.urn;
                trackSavedProperty(
                    { type: EventType.EditStructuredPropertyEvent, propertyUrn: selectedProperty.urn },
                    allowedValues,
                );
            } else {
                const allowedTypes = formValues?.typeQualifier?.allowedTypes ?? [];
                const input = {
                    displayName: formValues?.displayName,
                    description: formValues?.description,
                    qualifiedName: formValues?.qualifiedName || undefined,
                    valueType: valueTypeUrn,
                    // Validation above guarantees at least one entity type.
                    entityTypes: formValues?.entityTypes ?? [],
                    allowedPlatforms: formValues?.allowedPlatforms,
                    immutable: formValues?.immutable,
                    allowedValues: allowedValues.length ? allowedValues : undefined,
                    typeQualifier: allowedTypes.length ? { allowedTypes } : undefined,
                    cardinality,
                    settings,
                };

                const result = await createStructuredProperty({ variables: { input } });
                savedPropertyUrn = result.data?.createStructuredProperty?.urn;
                trackSavedProperty({ type: EventType.CreateStructuredPropertyEvent }, allowedValues);
            }

            // The property itself is already saved here, so a badge failure must not be reported as
            // a failed save — the user would retry a create that already succeeded.
            let badgeReplaceFailed = false;
            try {
                await replaceAssetBadge({
                    existingBadgeUrn: badgeProperty?.urn,
                    savedPropertyUrn,
                    enableBadge: formValues?.settings?.showAsAssetBadge ?? false,
                    updateBadge: (propertyUrn, enabled) =>
                        updateStructuredProperty({
                            variables: { input: { urn: propertyUrn, settings: { showAsAssetBadge: enabled } } },
                        }),
                });
            } catch {
                badgeReplaceFailed = true;
            }

            finishSave(badgeReplaceFailed);
        } catch {
            showErrorMessage();
        } finally {
            setIsSubmitting(false);
        }
    };

    useEffect(() => {
        if (selectedProperty) {
            const typeValue = getValueType(
                selectedProperty.definition.valueType.urn,
                selectedProperty.definition.cardinality || PropertyCardinality.Single,
            );
            setFormValues({
                displayName: getDisplayName(selectedProperty),
                description: selectedProperty.definition.description,
                qualifiedName: selectedProperty.definition.qualifiedName,
                valueType: typeValue,
                entityTypes: selectedProperty.definition.entityTypes.map((entityType) => entityType.urn),
                allowedPlatforms: selectedProperty.definition.allowedPlatforms?.map((platform) => platform.urn),
                typeQualifier: {
                    allowedTypes: selectedProperty.definition.typeQualifier?.allowedTypes?.map(
                        (entityType) => entityType.urn,
                    ),
                },
                immutable: selectedProperty.definition.immutable,
                settings: selectedProperty.settings,
            });
            setSelectedValueType(typeValue ?? '');
            setCardinality(selectedProperty.definition.cardinality ?? PropertyCardinality.Single);
        } else if (!isEditMode) {
            setFormValues(undefined);
            setSelectedValueType('');
        }
    }, [selectedProperty, isEditMode]);

    useEffect(() => {
        if (isEditMode && !selectedProperty) return;

        const field = getStringOrNumberValueField(selectedValueType);
        setValueField(field);
        const propertyUrn = selectedProperty?.urn;
        const shouldHydrateRows =
            hydratedAllowedValues.current.propertyUrn !== propertyUrn ||
            hydratedAllowedValues.current.valueField !== field;
        hydratedAllowedValues.current = { propertyUrn, valueField: field };
        if (!shouldHydrateRows) return;

        const savedRows = selectedProperty?.definition?.allowedValues?.map((item) =>
            createAllowedValueRow({ [field]: item.value[field], description: item.description, isPersisted: true }),
        );
        setSavedAllowedValues(savedRows);
        // Rows only appear once the user explicitly clicks Add: allowed values are optional, and an
        // empty list means any value of the chosen type is accepted.
        setAllowedValueRows(savedRows ?? []);
    }, [isEditMode, selectedProperty, selectedValueType]);

    const liveAllowedValues = useMemo(() => toAllowedValueInputs(allowedValueRows), [allowedValueRows]);

    if (isEditMode && loading) return <Loader />;
    if (isEditMode && !selectedProperty) return <Redirect to={PageRoutes.STRUCTURED_PROPERTIES} />;
    if (!isEditMode && !canEditProps) return <Redirect to={PageRoutes.STRUCTURED_PROPERTIES} />;

    let pageTitleKey = 'createTitle';
    if (isEditMode) pageTitleKey = isReadOnly ? 'viewTitle' : 'editTitle';
    const pageTitle = t(pageTitleKey);
    const breadcrumb = (
        <Breadcrumb
            items={[
                {
                    key: 'structured-properties',
                    label: t('page.title'),
                    href: PageRoutes.STRUCTURED_PROPERTIES,
                },
                {
                    key: 'form',
                    label: pageTitle,
                },
            ]}
        />
    );

    return (
        // Cancel and the nav sidebar both navigate via history, so the provider's Prompt covers
        // them too. Redirect handling is off while submitting so the post-save push isn't blocked.
        <DiscardUnsavedChangesConfirmationProvider
            enableRedirectHandling={!isSubmitting && !isSaveComplete && !isReadOnly}
            isDiscardPrimary
            confirmButtonText={tc('discard')}
            closeButtonText={tc('cancel')}
        >
            <DirtyStateSync isDirty={isDirty} />
            <FormPageContainer $isShowNavBarRedesign={isShowNavBarRedesign}>
                <ScrollArea>
                    <CenteredColumn>
                        {breadcrumb}
                        <TitleWrapper>
                            <PageTitle title={pageTitle} />
                        </TitleWrapper>
                        <StructuredPropsForm
                            selectedProperty={selectedProperty}
                            isReadOnly={isReadOnly}
                            formValues={formValues}
                            setFormValues={setFormValues}
                            setFieldValue={setFieldValue}
                            errors={errors}
                            setCardinality={setCardinality}
                            isEditMode={isEditMode}
                            selectedValueType={selectedValueType}
                            setSelectedValueType={setSelectedValueType}
                            savedAllowedValues={savedAllowedValues}
                            allowedValueRows={allowedValueRows}
                            liveAllowedValues={liveAllowedValues}
                            addAllowedValueRow={addAllowedValueRow}
                            updateAllowedValueRow={updateAllowedValueRow}
                            removeAllowedValueRow={removeAllowedValueRow}
                            moveAllowedValueRow={moveAllowedValueRow}
                            valueField={valueField}
                            badgeProperty={badgeProperty?.urn === selectedProperty?.urn ? undefined : badgeProperty}
                            onValuesChange={markDirty}
                        />
                    </CenteredColumn>
                </ScrollArea>
                <ActionsBar dataTestId="structured-props-actions-bar" fullWidth>
                    <Button variant="text" color="gray" onClick={returnToList}>
                        {isReadOnly ? tc('close') : tc('cancel')}
                    </Button>
                    {!isReadOnly && (
                        <Button
                            onClick={handleSubmit}
                            disabled={isSubmitting}
                            data-testid="structured-props-create-update-button"
                        >
                            {isEditMode ? tc('update') : tc('create')}
                        </Button>
                    )}
                </ActionsBar>
            </FormPageContainer>
        </DiscardUnsavedChangesConfirmationProvider>
    );
}
