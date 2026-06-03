import { BookmarkSimple } from '@phosphor-icons/react/dist/csr/BookmarkSimple';
import { BookmarksSimple } from '@phosphor-icons/react/dist/csr/BookmarksSimple';
import React, { useState } from 'react';
import styled from 'styled-components/macro';

import { EntitySubHeaderSection, GenericEntityProperties } from '@app/entity/shared/types';
import EntityMenuActions, { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { DeprecationIcon } from '@app/entityV2/shared/components/styled/DeprecationIcon';
import EntityTitleLoadingSection from '@app/entityV2/shared/containers/profile/header/EntityHeaderLoadingSection';
import EntityName from '@app/entityV2/shared/containers/profile/header/EntityName';
import IconColorPicker from '@app/entityV2/shared/containers/profile/header/IconPicker/IconColorPicker';
import PlatformHeaderIcons from '@app/entityV2/shared/containers/profile/header/PlatformContent/PlatformHeaderIcons';
import StructuredPropertyBadge from '@app/entityV2/shared/containers/profile/header/StructuredPropertyBadge';
import { getParentEntities } from '@app/entityV2/shared/containers/profile/header/getParentEntities';
import { getDisplayedEntityType, getEntityPlatforms } from '@app/entityV2/shared/containers/profile/header/utils';
import { EntityBackButton } from '@app/entityV2/shared/containers/profile/sidebar/EntityBackButton';
import EntityActions, { EntityActionItem } from '@app/entityV2/shared/entity/EntityActions';
import { DomainColoredIcon } from '@app/entityV2/shared/links/DomainColoredIcon';
import VersioningBadge from '@app/entityV2/shared/versioning/VersioningBadge';
import GlossaryColoredIcon from '@app/glossaryV2/GlossaryColoredIcon';
import { useGenerateGlossaryColorFromPalette } from '@app/glossaryV2/colorUtils';
import ContextPath from '@app/previewV2/ContextPath';
import HealthIcon from '@app/previewV2/HealthIcon';
import NotesIcon from '@app/previewV2/NotesIcon';
import { useGenerateDomainColorFromPalette } from '@app/sharedV2/colors/colorUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { DataPlatform, DisplayProperties, Domain, EntityType, Post } from '@types';

const TitleWrapper = styled.div`
    min-width: 0;

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

const Row = styled.div`
    padding: 18px;
    display: flex;
    flex-direction: row;
    justify-content: space-between;
    align-items: center;
    position: relative;
    overflow: hidden;
`;

const LeftColumn = styled.div`
    min-width: 0;

    display: flex;
    justify-content: center;
    align-items: center;
`;

const RightColumn = styled.div`
    flex-shrink: 0;
    display: flex;
    flex-direction: column;
    align-items: end;
    justify-content: center;
    overflow: hidden;
    padding-left: 8px;
`;

const TopButtonsWrapper = styled.div`
    display: flex;
    justify-content: flex-end;
    gap: 8px;
    max-width: 100%;
    width: 100%;
`;

const HeaderIconsWrapper = styled.span`
    margin-right: 8px;
`;

const ClickableIconWrapper = styled.div`
    cursor: pointer;
    margin-right: 12px;
    border-radius: 10px;
    transition: opacity 0.15s ease;

    &:hover {
        opacity: 0.85;
    }
`;

type Props = {
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
    const generateGlossaryColor = useGenerateGlossaryColorFromPalette();
    const generateDomainColor = useGenerateDomainColorFromPalette();

    const displayedEntityType = getDisplayedEntityType(entityData, entityRegistry, entityType);
    const { platform, platforms } = getEntityPlatforms(entityType, entityData);

    const isGlossaryNode = entityType === EntityType.GlossaryNode;
    const isGlossaryTerm = entityType === EntityType.GlossaryTerm;
    const isGlossaryEntity = isGlossaryNode || isGlossaryTerm;
    const isDomainEntity = entityType === EntityType.Domain;

    // The color currently displayed in the UI: prefer the saved colorHex, otherwise the
    // deterministic palette color we generate from the URN. We pass this into the picker so
    // editing always starts from "the color you see" — not the gray default.
    let resolvedCurrentColor: string | undefined;
    if (isGlossaryEntity) {
        resolvedCurrentColor = displayProperties?.colorHex || generateGlossaryColor(urn);
    } else if (isDomainEntity) {
        resolvedCurrentColor = displayProperties?.colorHex || generateDomainColor(urn);
    } else {
        resolvedCurrentColor = displayProperties?.colorHex || undefined;
    }

    const contextPath = getParentEntities(entityData, entityType);
    return (
        <>
            <Row>
                <LeftColumn>
                    <EntityBackButton />
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
                                    <ClickableIconWrapper onClick={() => setShowIconPicker(true)}>
                                        {isGlossaryEntity ? (
                                            <GlossaryColoredIcon
                                                color={resolvedCurrentColor || ''}
                                                icon={isGlossaryTerm ? BookmarkSimple : BookmarksSimple}
                                                size={40}
                                            />
                                        ) : (
                                            <DomainColoredIcon domain={entityData as Domain} />
                                        )}
                                    </ClickableIconWrapper>
                                )}
                                {showIconPicker && (
                                    <IconColorPicker
                                        name={entityRegistry.getDisplayName(entityType, entityData)}
                                        open={showIconPicker}
                                        onClose={() => setShowIconPicker(false)}
                                        color={resolvedCurrentColor}
                                        icon={displayProperties?.icon?.name}
                                        showIcon={!!isIconEditable}
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
                        {headerDropdownItems && (
                            <EntityMenuActions
                                menuItems={headerDropdownItems}
                                shouldExternalLinksFillAllAvailableSpace={!headerActionItems}
                            />
                        )}
                    </TopButtonsWrapper>
                </RightColumn>
            </Row>
        </>
    );
};
