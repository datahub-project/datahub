import { getContextPath } from '@app/entityV2/shared/containers/profile/header/getContextPath';
import VersioningBadge from '@app/entityV2/shared/versioning/VersioningBadge';
import { Divider } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components/macro';
import {
    Container,
    DataPlatform,
    DisplayProperties,
    Domain,
    EntityType,
    Post,
} from '../../../../../../types.generated';
import { EntitySubHeaderSection, GenericEntityProperties } from '../../../../../entity/shared/types';
import ContextPath from '../../../../../previewV2/ContextPath';
import HealthIcon from '../../../../../previewV2/HealthIcon';
import NotesIcon from '../../../../../previewV2/NotesIcon';
import useContentTruncation from '../../../../../shared/useContentTruncation';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { IconStyleType } from '../../../../Entity';
import EntityMenuActions, { EntityMenuItems } from '../../../EntityDropdown/EntityMenuActions';
import { DeprecationIcon } from '../../../components/styled/DeprecationIcon';
import EntityActions, { EntityActionItem } from '../../../entity/EntityActions';
import { DomainColoredIcon } from '../../../links/DomainColoredIcon';
import { EntityBackButton } from '../sidebar/EntityBackButton';
import EntityTitleLoadingSection from './EntityHeaderLoadingSection';
import EntityName from './EntityName';
import { GlossaryPreviewCardDecoration } from './GlossaryPreviewCardDecoration';
import IconColorPicker from './IconPicker/IconColorPicker';
import ContainerIcon from './PlatformContent/ContainerIcon';
import PlatformHeaderIcons from './PlatformContent/PlatformHeaderIcons';
import StructuredPropertyBadge from './StructuredPropertyBadge';
import { getDisplayedEntityType, getEntityPlatforms } from './utils';

export const TitleWrapper = styled.div`
    display: flex;
    justify-content: start;
    align-items: center;
    padding: 0px 0px 0px 0px;

    .ant-typography-edit-content {
        padding-top: 7px;
        margin-left: 15px;
    }
`;
const EntityDetailsContainer = styled.div`
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
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: start;
    flex-grow: 1;
    flex-shrink: 1;
`;

export const RightColumn = styled.div`
    display: flex;
    flex-direction: column;
    align-items: end;
    justify-content: center;
`;

export const TopButtonsWrapper = styled.div`
    display: flex;
    justify-content: flex-end;
    gap: 8px;
    max-width: 100%;
`;

export const StyledDivider = styled(Divider)`
    &&& {
        margin: 0px;
        padding: 0px;
    }
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

    const { contentRef, isContentTruncated } = useContentTruncation(entityData);
    const typeIcon =
        entityType === EntityType.Container ? (
            <ContainerIcon container={entityData as Container} />
        ) : (
            entityRegistry.getIcon(entityType, 12, IconStyleType.ACCENT)
        );

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
                                            instanceId={entityData?.dataPlatformInstance?.instanceId}
                                            typeIcon={typeIcon}
                                            type={displayedEntityType}
                                            entityType={entityType}
                                            browsePaths={entityData?.browsePathV2}
                                            parentEntities={contextPath}
                                            contentRef={contentRef}
                                            isContentTruncated={isContentTruncated}
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
