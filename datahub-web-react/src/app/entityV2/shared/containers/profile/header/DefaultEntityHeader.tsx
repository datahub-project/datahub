import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { Divider } from 'antd';
import { EntityHealth } from './EntityHealth';
import EntityName from './EntityName';
import { DeprecationPill } from '../../../components/styled/DeprecationPill';
import EntityActions, { EntityActionItem } from '../../../entity/EntityActions';
import EntityTitleLoadingSection from './EntityHeaderLoadingSection';
import IconColorPicker from './IconPicker/IconColorPicker';
import { EntitySubHeaderSection } from '../../../types';
import { Container, DisplayProperties, Domain, EntityType } from '../../../../../../types.generated';
import { DomainColoredIcon } from '../../../links/DomainColoredIcon';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { EntityBackButton } from '../sidebar/EntityBackButton';
import EntityMenuActions, { EntityMenuItems } from '../../../EntityDropdown/EntityMenuActions';
import { EntityHeaderDecoration } from './EntityHeaderDecoration';
import PlatformHeaderIcons from './PlatformContent/PlatformHeaderIcons';
import { IconStyleType } from '../../../../Entity';
import SearchCardBrowsePath from '../../../../../previewV2/SearchCardBrowsePath';
import useContentTruncation from '../../../../../shared/useContentTruncation';
import { getDisplayedEntityType } from './utils';
import ContainerIcon from './PlatformContent/ContainerIcon';

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
    gap: 5px;
`;

const DetailColumn = styled.div`
    display: flex;
    flex-direction: row;
`;
export const Row = styled.div`
    padding: 14px 14px 14px 26px;
    display: flex;
    flex-direction: row;
    justify-content: space-between;
    align-items: center;
    position: relative;
    overflow: hidden;
`;

export const PlatformRow = styled(Row)`
    padding: 0px;
`;

export const LeftColumn = styled.div`
    flex: 1;
    width: 70%;
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: start;
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
`;

export const StyledDivider = styled(Divider)`
    &&& {
        margin: 0px;
        padding: 0px;
    }
`;

export type Props = {
    urn: string;
    entityType: EntityType;
    entityUrl: string;
    loading: boolean;
    entityData?: any;
    refetch: () => void;
    headerActionItems?: Set<EntityActionItem>;
    headerDropdownItems?: Set<EntityMenuItems>;
    subHeader?: EntitySubHeaderSection;
    showEditName?: boolean;
    isColorEditable?: boolean;
    isIconEditable?: boolean;
    displayProperties?: DisplayProperties;
};

// returns book icon for glossary term, otherwise returns default icon for entity type
export const getDefaultIconForEntityType = (entityType: EntityType): string => {
    switch (entityType) {
        case EntityType.GlossaryNode:
            return 'Book';
        case EntityType.Domain:
            return 'Workspaces';
        default:
            return '';
    }
};

export const DefaultEntityHeader = ({
    urn,
    entityType,
    entityUrl,
    loading,
    entityData,
    refetch,
    headerDropdownItems,
    headerActionItems,
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    subHeader,
    showEditName,
    isColorEditable,
    isIconEditable,
    displayProperties,
}: Props) => {
    const [showIconPicker, setShowIconPicker] = useState(false);
    const entityRegistry = useEntityRegistry();

    const entityLogoComponent = entityRegistry.getIcon(entityType, 12, IconStyleType.ACCENT);

    const { contentRef, isContentTruncated } = useContentTruncation(entityData);
    const typeIcon =
        entityType === EntityType.Container ? (
            <ContainerIcon container={entityData as Container} />
        ) : (
            entityRegistry.getIcon(entityType, 12, IconStyleType.ACCENT)
        );

    const displayedEntityType = getDisplayedEntityType(entityData, entityRegistry, entityType);

    return (
        <>
            <Row>
                <EntityHeaderDecoration urn={urn} entityType={entityType} displayProperties={displayProperties} />
                <EntityBackButton />
                <LeftColumn>
                    {(loading && <EntityTitleLoadingSection />) || (
                        <>
                            <TitleWrapper>
                                <PlatformHeaderIcons
                                    platform={entityData?.platform}
                                    entityLogoComponent={entityLogoComponent}
                                    platforms={entityData?.siblingPlatforms}
                                />
                                {(isIconEditable || isColorEditable) && (
                                    <div
                                        style={{
                                            cursor: 'pointer',
                                            marginRight: 8,
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
                                    <DetailColumn>
                                        <EntityName isNameEditable={showEditName} />
                                        {entityData?.deprecation?.deprecated && (
                                            <DeprecationPill
                                                urn={urn}
                                                deprecation={entityData?.deprecation}
                                                showUndeprecate
                                                refetch={refetch}
                                            />
                                        )}
                                        {entityData?.health && (
                                            <EntityHealth health={entityData.health} baseUrl={entityUrl} />
                                        )}
                                    </DetailColumn>
                                    <DetailColumn>
                                        <SearchCardBrowsePath
                                            instanceId={entityData?.dataPlatformInstance?.instanceId}
                                            typeIcon={typeIcon}
                                            type={displayedEntityType}
                                            entityType={entityType}
                                            parentContainers={entityData?.parentContainers?.containers}
                                            parentEntities={entityData?.parentDomains?.domains}
                                            parentContainersRef={contentRef}
                                            areContainersTruncated={isContentTruncated}
                                        />
                                    </DetailColumn>
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
