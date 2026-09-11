import { Heading, Modal, Pill, Text } from '@components';
import { Divider } from 'antd';
import React, { useEffect, useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { extractTypeFromUrn } from '@app/entity/shared/utils';
import {
    getDisplayName as getStructuredPropertyDisplayName,
    isStructuredProperty,
} from '@app/govern/structuredProperties/utils';
import AvatarsGroup from '@app/permissions/AvatarsGroup';
import { FIELD_TYPES, RESOURCE_TYPE, RESOURCE_URN, TYPE, URN } from '@app/permissions/policy/constants';
import {
    convertLegacyResourceFilter,
    getFieldCondition,
    getFieldValues,
    mapResourceTypeToDisplayName,
} from '@app/permissions/policy/policyUtils';
import { CompactEntityNameComponent } from '@app/recommendations/renderer/component/CompactEntityNameComponent';
import { useIsGlossaryBasedPoliciesEnabled } from '@app/shared/hooks/useIsGlossaryBasedPoliciesEnabled';
import { useIsStructuredPropertiesInPoliciesEnabled } from '@app/shared/hooks/useIsStructuredPropertiesInPoliciesEnabled';
import { useAppConfig } from '@app/useAppConfig';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { useGetEntitiesQuery } from '@graphql/entity.generated';
import { useGetIngestionSourceNamesLazyQuery } from '@graphql/ingestion.generated';
import { Entity, EntityType, Maybe, Policy, PolicyMatchCondition, PolicyState, PolicyType } from '@types';

type PrivilegeOptionType = {
    type?: string;
    name?: Maybe<string>;
};

type Props = {
    policy: Omit<Policy, 'urn'>;
    open: boolean;
    onClose: () => void;
    privileges: PrivilegeOptionType[] | undefined;
};

const PolicyContainer = styled.div`
    padding-left: 20px;
    padding-right: 20px;
    > div {
        margin-bottom: 32px;
    }
`;

const ThinDivider = styled(Divider)`
    margin-top: 8px;
    margin-bottom: 8px;
`;

const Privileges = styled.div`
    & > div {
        margin-top: 5px !important;
    }
`;

const FieldHeaderContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 8px;
`;

const PropertyRow = styled.div`
    margin-bottom: 16px;
`;

const ValueLabelAndValuesContainer = styled.div`
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 8px;
    margin-top: 4px;
`;

const ValueLabel = styled.span`
    white-space: nowrap;
`;

/**
 * Component used for displaying the details about an existing Policy.
 */
export default function PolicyDetailsModal({ policy, open, onClose, privileges }: Props) {
    const { t } = useTranslation('settings.permissions');
    const { t: tc } = useTranslation('common.actions');
    const entityRegistry = useEntityRegistryV2();
    const isGlossaryBasedPoliciesEnabled = useIsGlossaryBasedPoliciesEnabled();
    const isStructuredPropertiesInPoliciesEnabled = useIsStructuredPropertiesInPoliciesEnabled();

    const isActive = policy?.state === PolicyState.Active;
    const isMetadataPolicy = policy?.type === PolicyType.Metadata;

    const resources = useMemo(() => convertLegacyResourceFilter(policy?.resources), [policy?.resources]);
    const resourceTypes = getFieldValues(resources?.filter, TYPE, RESOURCE_TYPE) || [];
    const dataPlatformInstances = getFieldValues(resources?.filter, 'DATA_PLATFORM_INSTANCE') || [];
    const resourceEntities = useMemo(
        () => getFieldValues(resources?.filter, URN, RESOURCE_URN) || [],
        [resources?.filter],
    );
    const resourceTypeCondition =
        getFieldCondition(resources?.filter, TYPE, RESOURCE_TYPE) || PolicyMatchCondition.Equals;
    const resourceFilterCondition =
        getFieldCondition(resources?.filter, URN, RESOURCE_URN) || PolicyMatchCondition.Equals;
    const domains = getFieldValues(resources?.filter, 'DOMAIN') || [];
    const domainCondition = getFieldCondition(resources?.filter, 'DOMAIN') || PolicyMatchCondition.Equals;
    const containers = getFieldValues(resources?.filter, 'CONTAINER') || [];
    const containerCondition = getFieldCondition(resources?.filter, 'CONTAINER') || PolicyMatchCondition.Equals;
    const tags = getFieldValues(resources?.filter, 'TAG') || [];
    const tagCondition = getFieldCondition(resources?.filter, 'TAG') || PolicyMatchCondition.Equals;
    const glossaryEntities = getFieldValues(resources?.filter, 'GLOSSARY') || [];
    const glossaryCondition = getFieldCondition(resources?.filter, 'GLOSSARY') || PolicyMatchCondition.Equals;
    const structuredPropertyCondition =
        getFieldCondition(resources?.filter, 'STRUCTURED_PROPERTY') || PolicyMatchCondition.Equals;
    const structuredProperties = useMemo(
        () =>
            resources?.filter?.criteria?.find((c) => c.field === FIELD_TYPES.STRUCTURED_PROPERTY)
                ?.structuredPropertyValues || [],
        [resources?.filter?.criteria],
    );

    const {
        config: { policiesConfig },
    } = useAppConfig();

    // Ingestion sources aren't in the entity registry and the policy query doesn't resolve
    // them into entities, so look their names up directly (same as the policy edit form).
    const [getIngestionSourceNames, { data: sourceNamesData }] = useGetIngestionSourceNamesLazyQuery();
    const ingestionSourceUrns = useMemo(
        () =>
            resourceEntities
                .map((value) => value.value)
                .filter((urn) => extractTypeFromUrn(urn) === EntityType.IngestionSource),
        [resourceEntities],
    );

    useEffect(() => {
        if (ingestionSourceUrns.length > 0) {
            getIngestionSourceNames({ variables: { urns: ingestionSourceUrns } });
        }
    }, [ingestionSourceUrns, getIngestionSourceNames]);

    const ingestionSourceNames = useMemo(() => {
        const sources = sourceNamesData?.listIngestionSources?.ingestionSources || [];
        return new Map(sources.map((source) => [source.urn, source.name]));
    }, [sourceNamesData]);

    // Extract property URNs from the policy with proper type safety
    const propertyUrns = useMemo(() => {
        const urns = new Set<string>();
        structuredProperties?.forEach((prop) => {
            const propertyUrn = prop?.propertyUrn;
            if (propertyUrn && typeof propertyUrn === 'string' && propertyUrn.trim()) {
                urns.add(propertyUrn);
            }
        });
        return Array.from(urns);
    }, [structuredProperties]);

    // Fetch only the structured properties used in this policy
    const { data: structuredPropertiesData } = useGetEntitiesQuery({
        skip: propertyUrns.length === 0,
        variables: { urns: propertyUrns },
    });

    const structuredPropertyNames = useMemo(() => {
        const nameMap = new Map<string, string>();
        if (!structuredPropertiesData?.entities) return nameMap;

        structuredPropertiesData.entities.filter(isStructuredProperty).forEach((entity) => {
            nameMap.set(entity.urn, getStructuredPropertyDisplayName(entity));
        });

        return nameMap;
    }, [structuredPropertiesData]);

    // Extract URNs from structured property values that might be entity references with proper type safety
    const propertyValueUrns = useMemo(() => {
        const urns = new Set<string>();
        structuredProperties?.forEach((prop) => {
            const values = prop?.values;
            if (Array.isArray(values)) {
                values.forEach((value) => {
                    if (typeof value === 'string' && value.startsWith('urn:li:')) {
                        urns.add(value);
                    }
                });
            }
        });
        return Array.from(urns);
    }, [structuredProperties]);

    const { data: entityData } = useGetEntitiesQuery({
        skip: propertyValueUrns.length === 0,
        variables: { urns: propertyValueUrns },
    });

    const entityValueMap = useMemo(() => {
        const valueMap = new Map<string, any>();
        if (!entityData?.entities) return valueMap;

        entityData.entities
            .filter((entity): entity is NonNullable<typeof entity> => entity != null)
            .forEach((entity) => {
                valueMap.set(entity.urn, entity);
            });

        return valueMap;
    }, [entityData]);

    const modalButtons = [
        {
            text: tc('close'),
            onClick: onClose,
        },
    ];

    const getDisplayName = (entity) => {
        if (!entity) {
            return null;
        }
        return entityRegistry.getDisplayName(entity.type, entity);
    };

    const getConditionLabel = (condition: PolicyMatchCondition) => {
        switch (condition) {
            case PolicyMatchCondition.Equals:
                return t('policyForm.condition.equals');
            case PolicyMatchCondition.NotEquals:
                return t('policyForm.condition.notEquals');
            case PolicyMatchCondition.StartsWith:
                return t('policyForm.condition.startsWith');
            default:
                return condition;
        }
    };

    const renderValueDisplay = (label: string, condition: PolicyMatchCondition, entity?: Maybe<Entity>) => {
        if (condition === PolicyMatchCondition.StartsWith) {
            return <Text size="md">{label}</Text>;
        }

        // Unregistered types (e.g. ingestion sources) get a plain pill.
        if (!entity || !entityRegistry.hasEntity(entity.type)) {
            return <Pill label={label} size="md" />;
        }

        // Registered entities use CompactEntityNameComponent for link + tooltip.
        return <CompactEntityNameComponent entity={entity} />;
    };

    const renderFieldWithCondition = (fieldLabel: string, condition: PolicyMatchCondition) => {
        return (
            <FieldHeaderContainer>
                <Heading type="h5" size="md" weight="bold" color="text">
                    {fieldLabel}
                </Heading>
                <Pill label={getConditionLabel(condition)} color="primary" size="sm" clickable={false} />
            </FieldHeaderContainer>
        );
    };

    const resourceOwnersField = (actors) => {
        if (!actors?.resourceOwners) {
            return <Pill label={t('details.ownersNo')} size="md" />;
        }
        if ((actors?.resolvedOwnershipTypes?.length ?? 0) > 0) {
            return (
                <div>
                    {actors?.resolvedOwnershipTypes?.map((type: any) => (
                        <Pill key={type.urn} label={type.info.name} size="sm" />
                    ))}
                </div>
            );
        }
        return <Pill label={t('details.ownersYesAll')} size="md" />;
    };

    return (
        <Modal title={policy?.name} open={open} onCancel={onClose} closable width={800} buttons={modalButtons}>
            <PolicyContainer>
                <div>
                    <Heading type="h5" size="md" weight="bold" color="text">
                        {t('column.type')}
                    </Heading>
                    <ThinDivider />
                    <Pill label={policy?.type} />
                </div>
                <div>
                    <Heading type="h5" size="md" weight="bold" color="text">
                        {t('column.state')}
                    </Heading>
                    <ThinDivider />
                    <Pill label={policy?.state} color={isActive ? 'green' : 'red'} />
                </div>
                <div>
                    <Heading type="h5" size="md" weight="bold" color="text">
                        {t('column.description')}
                    </Heading>
                    <ThinDivider />
                    <Text type="span" color="textSecondary">
                        {policy?.description || '-'}
                    </Text>
                </div>
                {isMetadataPolicy && (
                    <>
                        <div>
                            {renderFieldWithCondition(t('details.assetTypeLabel'), resourceTypeCondition)}
                            <ThinDivider />
                            {(resourceTypes?.length &&
                                resourceTypes.map((value) =>
                                    renderValueDisplay(
                                        mapResourceTypeToDisplayName(
                                            value.value,
                                            policiesConfig?.resourcePrivileges || [],
                                        ) || '',
                                        resourceTypeCondition,
                                    ),
                                )) || <Pill label={t('details.tagAll')} size="md" />}
                        </div>
                        <div>
                            {renderFieldWithCondition(t('details.assetsLabel'), resourceFilterCondition)}
                            <ThinDivider />
                            {(resourceEntities?.length &&
                                resourceEntities.map((value) =>
                                    renderValueDisplay(
                                        getDisplayName(value.entity) ||
                                            ingestionSourceNames.get(value.value) ||
                                            value.value,
                                        resourceFilterCondition,
                                        value.entity,
                                    ),
                                )) || <Pill label={t('details.tagAll')} size="md" />}
                        </div>
                        {dataPlatformInstances?.length > 0 && (
                            <div>
                                <Heading type="h5" size="md" weight="bold" color="text">
                                    {t('details.dataPlatformInstancesLabel')}
                                </Heading>
                                <ThinDivider />
                                {dataPlatformInstances.map((value, key) => (
                                    <Pill
                                        // eslint-disable-next-line react/no-array-index-key
                                        key={`dataPlatformInstance-${value.value}-${key}`}
                                        label={getDisplayName(value.entity) || value.value}
                                        size="md"
                                    />
                                ))}
                            </div>
                        )}
                        <div>
                            {renderFieldWithCondition(t('details.domainsLabel'), domainCondition)}
                            <ThinDivider />
                            {(domains?.length &&
                                domains.map((value) =>
                                    renderValueDisplay(
                                        getDisplayName(value.entity) || value.value,
                                        domainCondition,
                                        value.entity,
                                    ),
                                )) || <Pill label={t('details.tagAll')} size="md" />}
                        </div>
                        <div>
                            {renderFieldWithCondition(t('details.containersLabel'), containerCondition)}
                            <ThinDivider />
                            {(containers?.length &&
                                containers.map((value) =>
                                    renderValueDisplay(
                                        getDisplayName(value.entity) || value.value,
                                        containerCondition,
                                        value.entity,
                                    ),
                                )) || <Pill label={t('details.tagAll')} size="md" />}
                        </div>
                        <div>
                            {renderFieldWithCondition(t('details.tagsLabel'), tagCondition)}
                            <ThinDivider />
                            {(tags?.length &&
                                tags.map((value) =>
                                    renderValueDisplay(
                                        getDisplayName(value.entity) || value.value,
                                        tagCondition,
                                        value.entity,
                                    ),
                                )) || <Pill label={t('details.tagAll')} size="md" />}
                        </div>
                        {isGlossaryBasedPoliciesEnabled && (
                            <div>
                                {renderFieldWithCondition(t('details.glossaryTermsLabel'), glossaryCondition)}
                                <ThinDivider />
                                {(glossaryEntities?.length &&
                                    glossaryEntities.map((value) =>
                                        renderValueDisplay(
                                            getDisplayName(value.entity) || value.value,
                                            glossaryCondition,
                                            value.entity,
                                        ),
                                    )) || <Pill label={t('details.tagAll')} size="md" />}
                            </div>
                        )}
                        {isStructuredPropertiesInPoliciesEnabled && (
                            <div>
                                {renderFieldWithCondition(
                                    t('details.structuredPropertiesLabel'),
                                    structuredPropertyCondition,
                                )}
                                <ThinDivider />
                                {(structuredProperties?.length > 0 && (
                                    <>
                                        {structuredProperties.map((prop, index) => {
                                            // Use stable key combining URN with first value to avoid duplicates
                                            // eslint-disable-next-line react/no-array-index-key
                                            const rowKey = `${prop?.propertyUrn || ''}-${prop?.values?.[0] || ''}-${index}`;
                                            return (
                                                <PropertyRow key={rowKey}>
                                                    <Text type="span" color="textSecondary">
                                                        <strong>{t('details.structuredPropertyLabel')}:</strong>{' '}
                                                        {structuredPropertyNames.get(prop?.propertyUrn) ||
                                                            prop?.propertyUrn}
                                                    </Text>
                                                    <ValueLabelAndValuesContainer>
                                                        <ValueLabel>
                                                            <Text type="span" color="textSecondary">
                                                                <strong>
                                                                    {t('details.structuredPropertyValuesLabel')}:
                                                                </strong>
                                                            </Text>
                                                        </ValueLabel>
                                                        {prop?.values?.map((value, valueIndex) => {
                                                            const isEntityUrn = value?.startsWith('urn:li:');
                                                            const entity = isEntityUrn
                                                                ? entityValueMap.get(value)
                                                                : null;
                                                            // Use valueIndex as discriminator to ensure unique keys for duplicate properties
                                                            // eslint-disable-next-line react/no-array-index-key
                                                            const valueKey = `${prop.propertyUrn}-${value}-${valueIndex}`;

                                                            if (entity) {
                                                                return (
                                                                    <CompactEntityNameComponent
                                                                        key={valueKey}
                                                                        entity={entity}
                                                                        showFullTooltip
                                                                        showMargin={false}
                                                                    />
                                                                );
                                                            }

                                                            return <Pill key={valueKey} label={value} size="md" />;
                                                        })}
                                                    </ValueLabelAndValuesContainer>
                                                </PropertyRow>
                                            );
                                        })}
                                    </>
                                )) || <Text>-</Text>}
                            </div>
                        )}
                    </>
                )}
                <Privileges>
                    <Heading type="h5" size="md" weight="bold" color="text">
                        {t('privilegesLabel')}
                    </Heading>
                    <ThinDivider />
                    {privileges?.map((priv, key) => (
                        // eslint-disable-next-line react/no-array-index-key
                        <Pill key={`${priv}-${key}`} label={priv?.name || ''} size="sm" />
                    ))}
                </Privileges>
                <div>
                    <Heading type="h5" size="md" weight="bold" color="text">
                        {t('details.appliesToOwnersLabel')}
                    </Heading>
                    <ThinDivider />
                    {resourceOwnersField(policy?.actors)}
                </div>
                <div>
                    <Heading type="h5" size="md" weight="bold" color="text">
                        {t('details.appliesToUsersLabel')}
                    </Heading>
                    <ThinDivider />
                    {policy?.actors?.allUsers ? (
                        <Pill label={t('allUsers')} size="md" />
                    ) : (
                        <AvatarsGroup
                            users={policy?.actors?.resolvedUsers}
                            entityRegistry={entityRegistry}
                            maxCount={50}
                            title=""
                        />
                    )}
                </div>
                <div>
                    <Heading type="h5" size="md" weight="bold" color="text">
                        {t('details.appliesToGroupsLabel')}
                    </Heading>
                    <ThinDivider />
                    {policy?.actors?.allGroups ? (
                        <Pill label={t('allGroups')} size="md" />
                    ) : (
                        <AvatarsGroup
                            groups={policy?.actors?.resolvedGroups}
                            entityRegistry={entityRegistry}
                            maxCount={50}
                            title=""
                        />
                    )}
                </div>
                <div>
                    <Heading type="h5" size="md" weight="bold" color="text">
                        {t('details.appliesToRolesLabel')}
                    </Heading>
                    <ThinDivider />
                    <AvatarsGroup
                        roles={policy?.actors?.resolvedRoles}
                        entityRegistry={entityRegistry}
                        maxCount={50}
                        title=""
                    />
                </div>
            </PolicyContainer>
        </Modal>
    );
}
