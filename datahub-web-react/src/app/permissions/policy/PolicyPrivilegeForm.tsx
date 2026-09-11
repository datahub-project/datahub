import { Text } from '@components';
import { Form } from 'antd';
import React, { useEffect, useMemo, useRef, useState } from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import GlossarySelector from '@app/permissions/policy/GlossarySelector';
import ContainersSelect from '@app/permissions/policy/PolicyPrivilegeForm/ContainersSelect';
import DomainsSelect from '@app/permissions/policy/PolicyPrivilegeForm/DomainsSelect';
import PrivilegesSelect from '@app/permissions/policy/PolicyPrivilegeForm/PrivilegesSelect';
import ResourceSelect from '@app/permissions/policy/PolicyPrivilegeForm/ResourceSelect';
import ResourceTypeSelect from '@app/permissions/policy/PolicyPrivilegeForm/ResourceTypeSelect';
import StructuredPropertyResourceSelect from '@app/permissions/policy/PolicyPrivilegeForm/StructuredPropertyResourceSelect';
import TagsSelect from '@app/permissions/policy/PolicyPrivilegeForm/TagsSelect';
import { FIELD_TYPES, RESOURCE_TYPE, RESOURCE_URN, TYPE, URN } from '@app/permissions/policy/constants';
import {
    EMPTY_POLICY,
    convertLegacyResourceFilter,
    createCriterionValue,
    createCriterionValueWithEntity,
    getFieldCondition,
    getFieldValues,
    getFieldValuesOfTags,
    mapResourceTypeToPrivileges,
    setFieldCondition,
    setFieldValues,
} from '@app/permissions/policy/policyUtils';
import { useIsGlossaryBasedPoliciesEnabled } from '@app/shared/hooks/useIsGlossaryBasedPoliciesEnabled';
import { useIsStructuredPropertiesInPoliciesEnabled } from '@app/shared/hooks/useIsStructuredPropertiesInPoliciesEnabled';
import { useAppConfig } from '@app/useAppConfig';

import { PolicyMatchCondition, PolicyType, ResourceFilter } from '@types';

const ALL_PRIVILEGES_VALUE = 'All';

type Props = {
    policyType: PolicyType;
    resources?: ResourceFilter;
    setResources: (resources: ResourceFilter) => void;
    setEditState: (data: boolean) => void;
    isEditState: boolean;
    privileges: Array<string>;
    setPrivileges: (newPrivs: Array<string>) => void;
    focusPolicyUrn: string | undefined;
};

const PrivilegesForm = styled(Form)`
    margin: 12px;
    margin-top: 36px;
    margin-bottom: 40px;
`;

const DescriptionParagraph = styled(Text)`
    && {
        display: block;
        margin-bottom: 8px;
    }
`;

/**
 * Component used to construct the "privileges" and "resources" portion of a DataHub
 * access Policy.
 */
export default function PolicyPrivilegeForm({
    policyType,
    resources: maybeResources,
    setResources,
    privileges,
    setPrivileges,
    setEditState,
    isEditState,
    focusPolicyUrn,
}: Props) {
    const { t } = useTranslation('settings.permissions');
    const isGlossaryBasedPoliciesEnabled = useIsGlossaryBasedPoliciesEnabled();
    const isStructuredPropertiesInPoliciesEnabled = useIsStructuredPropertiesInPoliciesEnabled();
    const normalizedRef = useRef(false);
    const [conditions, setConditions] = useState<Record<string, PolicyMatchCondition>>({
        RESOURCE_TYPE: PolicyMatchCondition.Equals,
        RESOURCE: PolicyMatchCondition.Equals,
        TAG: PolicyMatchCondition.Equals,
        DOMAIN: PolicyMatchCondition.Equals,
        CONTAINER: PolicyMatchCondition.Equals,
        GLOSSARY: PolicyMatchCondition.Equals,
        STRUCTURED_PROPERTY: PolicyMatchCondition.Equals,
    });

    const updateCondition = (fieldType: string, condition: PolicyMatchCondition) => {
        setConditions((prev) => ({ ...prev, [fieldType]: condition }));
    };

    // Configuration used for displaying options
    const {
        config: { policiesConfig },
    } = useAppConfig();

    // Memoized: convertLegacyResourceFilter returns a new object for legacy (filter-less)
    // policies, and downstream effects key on resources.filter identity — an unstable
    // reference would re-fire them every render.
    const resources: ResourceFilter = useMemo(
        () => convertLegacyResourceFilter(maybeResources) || EMPTY_POLICY.resources,
        [maybeResources],
    );

    // Normalize legacy field names (RESOURCE_TYPE → TYPE, RESOURCE_URN → URN) once when filter loads.
    // This prevents confusion when policies with old field names are edited and conditions
    // are synced from both old and new field names. Uses a ref to ensure normalization happens
    // only once, even if resources.filter changes.
    useEffect(() => {
        if (normalizedRef.current || !resources.filter?.criteria) return;

        const hasLegacyFields = resources.filter.criteria.some(
            (c) => c.field === RESOURCE_TYPE || c.field === RESOURCE_URN,
        );
        if (!hasLegacyFields) {
            // No legacy fields, mark as normalized to skip future checks
            normalizedRef.current = true;
            return;
        }

        const normalizedCriteria = resources.filter.criteria.map((criterion) => {
            if (criterion.field === RESOURCE_TYPE) {
                return { ...criterion, field: TYPE };
            }
            if (criterion.field === RESOURCE_URN) {
                return { ...criterion, field: URN };
            }
            return criterion;
        });

        // Only update if normalization actually changed something
        const changed = normalizedCriteria.some((c, i) => c.field !== resources.filter?.criteria?.[i]?.field);
        if (changed) {
            setResources({
                ...resources,
                filter: {
                    ...resources.filter,
                    criteria: normalizedCriteria,
                },
            });
        }
        normalizedRef.current = true;
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [resources.filter]);

    // RESOURCE_TYPE and RESOURCE_URN are deprecated, but need to get them for backwards compatibility
    const resourceTypes = useMemo(
        () => getFieldValues(resources.filter, TYPE, RESOURCE_TYPE) || [],
        [resources.filter],
    );

    const resourceEntities = useMemo(() => {
        return getFieldValues(resources.filter, URN, RESOURCE_URN) || [];
    }, [resources.filter]);

    useEffect(() => {
        const fieldTypeMap: Array<[string, string, string | null]> = [
            ['RESOURCE_TYPE', FIELD_TYPES.RESOURCE_TYPE, RESOURCE_TYPE],
            ['RESOURCE', FIELD_TYPES.RESOURCE, RESOURCE_URN],
            ['TAG', FIELD_TYPES.TAG, null],
            ['DOMAIN', FIELD_TYPES.DOMAIN, null],
            ['CONTAINER', FIELD_TYPES.CONTAINER, null],
            ['GLOSSARY', FIELD_TYPES.GLOSSARY, null],
            [FIELD_TYPES.STRUCTURED_PROPERTY, FIELD_TYPES.STRUCTURED_PROPERTY, null],
        ];

        setConditions((prev) => {
            const newConditions = { ...prev };
            fieldTypeMap.forEach(([key, fieldType, legacyField]) => {
                const cond = legacyField
                    ? getFieldCondition(resources.filter, fieldType, legacyField)
                    : getFieldCondition(resources.filter, fieldType);
                if (cond) {
                    newConditions[key] = cond;
                }
            });
            // Return the previous object when nothing changed so this effect can never
            // cause a render loop, even if resources.filter identity churns.
            const changed = Object.keys(newConditions).some((key) => newConditions[key] !== prev[key]);
            return changed ? newConditions : prev;
        });
    }, [resources.filter]);

    // Get containers from filter
    const containers = getFieldValues(resources.filter, FIELD_TYPES.CONTAINER) || [];

    // Whether to show the resource filter inputs including "resource type", "resource", and "domain"
    const showResourceFilterInput = policyType !== PolicyType.Platform;

    // Current Select dropdown values
    const resourceTypeSelectValue = useMemo(
        () => resourceTypes.map((criterionValue) => criterionValue.value),
        [resourceTypes],
    );
    const resourceSelectValue = resourceEntities.map((criterionValue) => criterionValue.value);
    const domainSelectValue = getFieldValues(resources.filter, FIELD_TYPES.DOMAIN).map(
        (criterionValue) => criterionValue.value,
    );
    const containerSelectValue = getFieldValues(resources.filter, FIELD_TYPES.CONTAINER).map(
        (criterionValue) => criterionValue.value,
    );

    // Construct privilege options for dropdown, deduplicating by type
    const platformPrivileges = useMemo(() => {
        const privs = policiesConfig?.platformPrivileges || [];
        // Deduplicate by type, keeping first occurrence
        const seen = new Set<string>();
        return privs.filter((priv) => {
            if (seen.has(priv.type)) {
                return false;
            }
            seen.add(priv.type);
            return true;
        });
    }, [policiesConfig]);
    const resourcePrivileges = useMemo(() => policiesConfig?.resourcePrivileges || [], [policiesConfig]);
    const resourcePrivilegesForType = useMemo(
        () => mapResourceTypeToPrivileges(resourceTypeSelectValue, resourcePrivileges),
        [resourceTypeSelectValue, resourcePrivileges],
    );
    const privilegeOptions = policyType === PolicyType.Platform ? platformPrivileges : resourcePrivilegesForType;

    // Labels resolved from every known privilege, since privilegeOptions only covers the currently selected resource types.
    const selectedPrivilegeOptions = useMemo(() => {
        const displayNameByType = new Map(
            [...platformPrivileges, ...resourcePrivileges.flatMap((resource) => resource.privileges)].map((priv) => [
                priv.type,
                priv.displayName,
            ]),
        );
        return privileges.map((type) => ({ value: type, label: displayNameByType.get(type) || type }));
    }, [privileges, platformPrivileges, resourcePrivileges]);

    // When a privilege is selected, add its type to the privileges list
    const onSelectPrivilege = (privilege: string) => {
        if (privilege === ALL_PRIVILEGES_VALUE) {
            setPrivileges(privilegeOptions.map((priv) => priv.type) as never[]);
        } else if (!privileges.includes(privilege)) {
            // Only add if not already present
            const newPrivs = [...privileges, privilege];
            setPrivileges(newPrivs as never[]);
        }
    };

    // When a privilege is deselected, remove its type from the privileges list
    const onDeselectPrivilege = (privilege: string) => {
        if (privilege === ALL_PRIVILEGES_VALUE) {
            setPrivileges([]);
        } else {
            const newPrivs = privileges.filter((priv) => priv !== privilege);
            setPrivileges(newPrivs);
        }
    };

    const handleResourceTypesChange = (newResourceTypes: string[]) => {
        const filter = resources.filter || {
            criteria: [],
        };
        // remove the deprecated RESOURCE_TYPE field and replace with TYPE field
        const filterWithoutDeprecatedField = setFieldValues(filter, RESOURCE_TYPE, []);
        const updatedCriterionValues = newResourceTypes.map((type) => createCriterionValue(type));
        let updatedFilter = setFieldValues(filterWithoutDeprecatedField, TYPE, updatedCriterionValues);
        updatedFilter = setFieldCondition(updatedFilter, FIELD_TYPES.RESOURCE_TYPE, conditions.RESOURCE_TYPE);
        setResources({
            ...resources,
            filter: updatedFilter,
        });
    };

    // When a resource is selected, add its urn to the list of resources
    const handleResourcesChange = (newResourceUrns: string[]) => {
        const filter = resources.filter || {
            criteria: [],
        };
        // remove the deprecated RESOURCE_URN field and replace with URN field
        const filterWithoutDeprecatedField = setFieldValues(filter, RESOURCE_URN, []);
        const updatedCriterionValues = newResourceUrns.map((urn) => createCriterionValueWithEntity(urn, null));
        let updatedFilter = setFieldValues(filterWithoutDeprecatedField, URN, updatedCriterionValues);
        updatedFilter = setFieldCondition(updatedFilter, FIELD_TYPES.RESOURCE, conditions.RESOURCE);
        setResources({
            ...resources,
            filter: updatedFilter,
        });
    };

    // Handle domain selection changes
    const onDomainsChange = (domainUrns: string[]) => {
        const filter = resources.filter || {
            criteria: [],
        };
        const updatedDomains = domainUrns.map((urn) => createCriterionValueWithEntity(urn, null));
        const updatedFilter = setFieldValues(filter, FIELD_TYPES.DOMAIN, updatedDomains, conditions.DOMAIN);
        setResources({
            ...resources,
            filter: updatedFilter,
        });
    };

    // Handle container selection changes
    const onContainersChange = (containerUrns: string[]) => {
        const filter = resources.filter || {
            criteria: [],
        };
        const updatedContainers = containerUrns.map((urn) => createCriterionValueWithEntity(urn, null));
        const updatedFilter = setFieldValues(filter, FIELD_TYPES.CONTAINER, updatedContainers, conditions.CONTAINER);
        setResources({
            ...resources,
            filter: updatedFilter,
        });
    };

    const tagUrns = getFieldValues(resources.filter, FIELD_TYPES.TAG).map((criterionValue) => {
        return typeof criterionValue === 'string' ? criterionValue : criterionValue?.value || criterionValue;
    });

    const editTags = getFieldValuesOfTags(resources.filter, FIELD_TYPES.TAG).map((criterionValue) => {
        if (criterionValue?.value) {
            return criterionValue?.entity;
        }
        return criterionValue;
    });
    useEffect(() => {
        if (focusPolicyUrn && isEditState && setEditState && editTags && tagUrns) {
            setEditState(false);
            const filter = resources.filter || {
                criteria: [],
            };
            setResources({
                ...resources,
                filter: setFieldValues(filter, FIELD_TYPES.TAG, [...(tagUrns as any)]),
            });
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [focusPolicyUrn, isEditState, setEditState, editTags, tagUrns]);

    const handleTagsChange = (tagUrnsUrns: string[]) => {
        const filter = resources.filter || {
            criteria: [],
        };
        const updatedFilter = setFieldValues(filter, FIELD_TYPES.TAG, tagUrnsUrns as any, conditions.TAG);
        setResources({
            ...resources,
            filter: updatedFilter,
        });
    };

    const handleConditionChange =
        (fieldType: string) => (condition: PolicyMatchCondition, updatedResources: ResourceFilter) => {
            updateCondition(fieldType, condition);
            setResources(updatedResources);
        };

    const handleStructuredPropertiesChange = (properties: Array<{ propertyUrn: string; values: string[] }>) => {
        const filter = resources.filter || {
            criteria: [],
        };
        if (properties.length === 0) {
            const updatedFilter = {
                ...filter,
                criteria: filter.criteria?.filter((c) => c.field !== FIELD_TYPES.STRUCTURED_PROPERTY) || [],
            };
            setResources({ ...resources, filter: updatedFilter });
        } else {
            const criterion = {
                field: FIELD_TYPES.STRUCTURED_PROPERTY,
                values: [],
                structuredPropertyValues: properties,
                condition: conditions.STRUCTURED_PROPERTY,
            };
            const otherCriteria = filter.criteria?.filter((c) => c.field !== FIELD_TYPES.STRUCTURED_PROPERTY) || [];
            const updatedFilter = { ...filter, criteria: [...otherCriteria, criterion] };
            setResources({
                ...resources,
                filter: updatedFilter,
            });
        }
    };

    const structuredProperties = useMemo(() => {
        const criterion = resources.filter?.criteria?.find((c) => c.field === FIELD_TYPES.STRUCTURED_PROPERTY);
        return criterion?.structuredPropertyValues || [];
    }, [resources.filter]);

    return (
        <PrivilegesForm layout="vertical">
            {showResourceFilterInput && (
                <Form.Item label={<Text weight="bold">{t('privilegeForm.resourceTypeLabel')}</Text>} labelAlign="right">
                    <DescriptionParagraph type="p" color="textSecondary">
                        <Trans t={t} i18nKey="privilegeForm.resourceTypeDescription" components={{ bold: <b /> }} />
                    </DescriptionParagraph>
                    <ResourceTypeSelect
                        resourceTypeSelectValue={resourceTypeSelectValue}
                        resourceTypes={resourceTypes}
                        resourceTypeCondition={conditions.RESOURCE_TYPE}
                        onConditionChange={handleConditionChange('RESOURCE_TYPE')}
                        onResourceTypesChange={handleResourceTypesChange}
                        resources={resources}
                        resourcePrivileges={resourcePrivileges}
                    />
                </Form.Item>
            )}
            {showResourceFilterInput && (
                <Form.Item label={<Text weight="bold">{t('privilegeForm.resourceLabel')}</Text>}>
                    <DescriptionParagraph type="p" color="textSecondary">
                        <Trans t={t} i18nKey="privilegeForm.resourceDescription" components={{ bold: <b /> }} />
                    </DescriptionParagraph>
                    <ResourceSelect
                        resourceSelectValue={resourceSelectValue}
                        resourceEntities={resourceEntities}
                        resourceCondition={conditions.RESOURCE}
                        onConditionChange={handleConditionChange('RESOURCE')}
                        onResourcesChange={handleResourcesChange}
                        resources={resources}
                        resourceTypeSelectValue={resourceTypeSelectValue}
                        resourcePrivileges={resourcePrivileges}
                        resourceTypeCondition={conditions.RESOURCE_TYPE}
                    />
                </Form.Item>
            )}
            {showResourceFilterInput && (
                <Form.Item label={<Text weight="bold">{t('privilegeForm.tagsLabel')}</Text>}>
                    <DescriptionParagraph type="p" color="textSecondary">
                        {t('privilegeForm.tagsDescription')}
                    </DescriptionParagraph>
                    <TagsSelect
                        tags={tagUrns as string[]}
                        tagCondition={conditions.TAG}
                        onConditionChange={handleConditionChange('TAG')}
                        onTagsChange={handleTagsChange}
                        resources={resources}
                    />
                </Form.Item>
            )}
            {showResourceFilterInput && (
                <Form.Item label={<Text weight="bold">{t('privilegeForm.domainsLabel')}</Text>}>
                    <DescriptionParagraph type="p" color="textSecondary">
                        <Trans t={t} i18nKey="privilegeForm.domainsDescription" components={{ bold: <b /> }} />
                    </DescriptionParagraph>
                    <DomainsSelect
                        domainSelectValue={domainSelectValue}
                        domainCondition={conditions.DOMAIN}
                        onConditionChange={handleConditionChange('DOMAIN')}
                        onDomainsChange={onDomainsChange}
                        resources={resources}
                    />
                </Form.Item>
            )}
            {showResourceFilterInput && (
                <Form.Item label={<Text weight="bold">{t('privilegeForm.containersLabel')}</Text>}>
                    <DescriptionParagraph type="p" color="textSecondary">
                        <Trans t={t} i18nKey="privilegeForm.containersDescription" components={{ bold: <b /> }} />
                    </DescriptionParagraph>
                    <ContainersSelect
                        containerSelectValue={containerSelectValue}
                        containers={containers}
                        containerCondition={conditions.CONTAINER}
                        onConditionChange={handleConditionChange('CONTAINER')}
                        onContainersChange={onContainersChange}
                        resources={resources}
                    />
                </Form.Item>
            )}
            {showResourceFilterInput && isGlossaryBasedPoliciesEnabled && (
                <Form.Item label={<Text weight="bold">{t('privilegeForm.glossaryTermsLabel')}</Text>}>
                    <GlossarySelector
                        resources={resources}
                        setResources={setResources}
                        glossaryCondition={conditions.GLOSSARY}
                        setGlossaryCondition={(condition) => updateCondition('GLOSSARY', condition)}
                    />
                </Form.Item>
            )}
            {showResourceFilterInput && isStructuredPropertiesInPoliciesEnabled && (
                <Form.Item label={<Text weight="bold">{t('privilegeForm.structuredPropertiesLabel')}</Text>}>
                    <DescriptionParagraph type="p" color="textSecondary">
                        <Trans
                            t={t}
                            i18nKey="privilegeForm.structuredPropertiesDescription"
                            components={{ bold: <b /> }}
                        />
                    </DescriptionParagraph>
                    <StructuredPropertyResourceSelect
                        structuredProperties={structuredProperties}
                        condition={conditions[FIELD_TYPES.STRUCTURED_PROPERTY]}
                        onConditionChange={handleConditionChange(FIELD_TYPES.STRUCTURED_PROPERTY)}
                        onStructuredPropertiesChange={handleStructuredPropertiesChange}
                        resources={resources}
                    />
                </Form.Item>
            )}
            <Form.Item label={<Text weight="bold">{t('privilegesLabel')}</Text>}>
                <DescriptionParagraph type="p" color="textSecondary">
                    {t('privilegeForm.privilegesDescription')}
                </DescriptionParagraph>
                <PrivilegesSelect
                    selectedPrivilegeOptions={selectedPrivilegeOptions}
                    privilegeOptions={privilegeOptions}
                    onSelectPrivilege={onSelectPrivilege}
                    onDeselectPrivilege={onDeselectPrivilege}
                />
            </Form.Item>
        </PrivilegesForm>
    );
}
