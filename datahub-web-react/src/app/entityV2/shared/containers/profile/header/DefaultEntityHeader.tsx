import React, { useState } from 'react';
import styled from 'styled-components/macro';

import { EntitySubHeaderSection, GenericEntityProperties } from '@app/entity/shared/types';
import EntityMenuActions, { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { DeprecationIcon } from '@app/entityV2/shared/components/styled/DeprecationIcon';
import EntityTitleLoadingSection from '@app/entityV2/shared/containers/profile/header/EntityHeaderLoadingSection';
import EntityName from '@app/entityV2/shared/containers/profile/header/EntityName';
import { GlossaryPreviewCardDecoration } from '@app/entityV2/shared/containers/profile/header/GlossaryPreviewCardDecoration';
import IconColorPicker from '@app/entityV2/shared/containers/profile/header/IconPicker/IconColorPicker';
import PlatformHeaderIcons from '@app/entityV2/shared/containers/profile/header/PlatformContent/PlatformHeaderIcons';
import StructuredPropertyBadge from '@app/entityV2/shared/containers/profile/header/StructuredPropertyBadge';
import { getContextPath } from '@app/entityV2/shared/containers/profile/header/getContextPath';
import { getDisplayedEntityType, getEntityPlatforms } from '@app/entityV2/shared/containers/profile/header/utils';
import { EntityBackButton } from '@app/entityV2/shared/containers/profile/sidebar/EntityBackButton';
import EntityActions, { EntityActionItem } from '@app/entityV2/shared/entity/EntityActions';
import { DomainColoredIcon } from '@app/entityV2/shared/links/DomainColoredIcon';
import VersioningBadge from '@app/entityV2/shared/versioning/VersioningBadge';
import ContextPath from '@app/previewV2/ContextPath';
import HealthIcon from '@app/previewV2/HealthIcon';
import NotesIcon from '@app/previewV2/NotesIcon';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { DataPlatform, DisplayProperties, Domain, EntityType, Post } from '@types';

export const TitleWrapper = styled.div`
    max-width: 100%;

    display: flex;
    align-items: center;
    justify-content: start;
    padding: 0;

    .ant-typography-edit-content {
        padding-top: 7px;
        margin-left: 15px;
    }
`;
const EntityDetailsContainer = styled.div`
    min-width: 0;

    display: flex;
    flex-direction: column;
    gap: 0px;
`;

const HeaderRow = styled.div`
    display: flex;
    flex-direction: row;
    align-items: center;
    gap: 8px;
`;

const TitleRow = styled(HeaderRow)`
    font-size: 16px;
`;

export const Row = styled.div`
    padding: 18px;
    display: flex;
    flex-direction: row;
    justify-content: space-between;
    align-items: center;
    position: relative;
    overflow: hidden;
`;

export const LeftColumn = styled.div`
    flex: 1;
    min-width: 0;

    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: start;
`;

export const RightColumn = styled.div`
    flex-shrink: 0;
    display: flex;
    flex-direction: column;
    align-items: end;
    justify-content: center;
    overflow: hidden;
    padding-left: 8px;
`;

export const TopButtonsWrapper = styled.div`
    display: flex;
    justify-content: flex-end;
    gap: 8px;
    max-width: 100%;
`;

const HeaderIconsWrapper = styled.span`
    margin-right: 8px;
`;

export type Props = {
    urn: string;
    entityType: EntityType;
    entityUrl: string;
    loading: boolean;
    entityData: GenericEntityProperties | null;
    refetch: () => void;
    headerActionItems?: Set<EntityActionItem>;
    headerDropdownItems?: Set<EntityMenuItems>;
    subHeader?: EntitySubHeaderSection;
    showEditName?: boolean;
    isColorEditable?: boolean;
    isIconEditable?: boolean;
    displayProperties?: DisplayProperties;
};

export const DefaultEntityHeader = ({
    urn,
    entityType,
    entityUrl,
    loading,
    entityData,
    refetch,
    headerDropdownItems,
    headerActionItems, // eslint-disable-next-line @typescript-eslint/no-unused-vars
    subHeader,
    showEditName,
    isColorEditable,
    isIconEditable,
    displayProperties,
}: Props) => {
    const [showIconPicker, setShowIconPicker] = useState(false);
    const entityRegistry = useEntityRegistry();

    const displayedEntityType = getDisplayedEntityType(entityData, entityRegistry, entityType);
    const { platform, platforms } = getEntityPlatforms(entityType, entityData);

    const contextPath = getContextPath(entityData);
    return (
        <>
            <Row>
                {!loading && (entityType === EntityType.GlossaryNode || entityType === EntityType.GlossaryTerm) && (
                    <GlossaryPreviewCardDecoration
                        urn={urn}
                        entityData={entityData}
                        displayProperties={displayProperties}
                    />
                )}
                <EntityBackButton />
                <LeftColumn>
                    {(loading && <EntityTitleLoadingSection />) || (
                        <>
                            <TitleWrapper>
                                <HeaderIconsWrapper>
                                    <PlatformHeaderIcons
                                        platform={platform as DataPlatform}
                                        platforms={platforms as DataPlatform[]}
                                    />
                                </HeaderIconsWrapper>
                                {(isIconEditable || isColorEditable) && (
                                    <div
                                        style={{
                                            cursor: 'pointer',
                                            marginRight: 12,
                                        }}
                                    >
                                        <DomainColoredIcon
                                            onClick={() => setShowIconPicker(true)}
                                            domain={entityData as Domain}
                                        />
                                    </div>
                                )}
                                {showIconPicker && (
                                    <IconColorPicker
                                        name={entityRegistry.getDisplayName(entityType, entityData)}
                                        open={showIconPicker}
                                        onClose={() => setShowIconPicker(false)}
                                        color={displayProperties?.colorHex}
                                        icon={displayProperties?.icon?.name}
                                    />
                                )}
                                <EntityDetailsContainer>
                                    <TitleRow>
                                        <EntityName isNameEditable={showEditName} />
                                        {!!entityData?.notes?.total && (
                                            <NotesIcon
                                                notes={entityData?.notes?.relationships?.map((r) => r.entity as Post)}
                                            />
                                        )}
                                        {entityData?.deprecation?.deprecated && (
                                            <DeprecationIcon
                                                urn={urn}
                                                deprecation={entityData?.deprecation}
                                                showUndeprecate
                                                refetch={refetch}
                                                showText={false}
                                            />
                                        )}
                                        {entityData?.health && (
                                            <HealthIcon urn={urn} health={entityData.health} baseUrl={entityUrl} />
                                        )}
                                        <StructuredPropertyBadge
                                            structuredProperties={entityData?.structuredProperties}
                                        />
                                        <VersioningBadge
                                            versionProperties={entityData?.versionProperties ?? undefined}
                                            showPopover
                                        />
                                    </TitleRow>
                                    <HeaderRow>
                                        <ContextPath
                                            displayedEntityType={displayedEntityType}
                                            entityType={entityType}
                                            browsePaths={entityData?.browsePathV2}
                                            parentEntities={contextPath}
                                        />
                                    </HeaderRow>
                                </EntityDetailsContainer>
                            </TitleWrapper>
                        </>
                    )}
                </LeftColumn>
                <RightColumn>
                    <TopButtonsWrapper>
                        {headerActionItems && (
                            <EntityActions urn={urn} actionItems={headerActionItems} refetchForEntity={refetch} />
                        )}
                        {headerDropdownItems && <EntityMenuActions menuItems={headerDropdownItems} />}
                    </TopButtonsWrapper>
                </RightColumn>
            </Row>
        </>
    );
};
