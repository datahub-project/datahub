import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { Divider } from 'antd';
import PlatformContent from './PlatformContent';
import { EntityHealth } from './EntityHealth';
import EntityName from './EntityName';
import { DeprecationPill } from '../../../components/styled/DeprecationPill';
import EntityActions, { EntityActionItem } from '../../../entity/EntityActions';
import EntityTitleLoadingSection from './EntityHeaderLoadingSection';
import EntityPlatformLoadingSection from './EntityPlatformLoadingSection';
import IconColorPicker from './IconPicker/IconColorPicker';
import { EntitySubHeaderSection } from '../../../types';
import { DisplayProperties, Domain, EntityType } from '../../../../../../types.generated';
import { DomainColoredIcon } from '../../../links/DomainColoredIcon';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { EntityBackButton } from '../sidebar/EntityBackButton';
import EntityMenuActions, { EntityMenuItems } from '../../../EntityDropdown/EntityMenuActions';
import { EntityHeaderDecoration } from './EntityHeaderDecoration';

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

export const Row = styled.div`
    padding: 18px 14px 18px 26px;
    display: flex;
    flex-direction: row;
    justify-content: space-between;
    align-items: flex-start;
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
    align-items: left;
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

// TODO: Fix the styles here to avoid requiring this.
const SubHeader = styled.div`
    padding: 0px 24px 8px 24px;
`;

const EntityTitleWrapper = styled.div`
    display: flex;
    flex-direction: column;
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
    subHeader,
    showEditName,
    isColorEditable,
    isIconEditable,
    displayProperties,
}: Props) => {
    const [showIconPicker, setShowIconPicker] = useState(false);
    const entityRegistry = useEntityRegistry();

    return (
        <>
            <Row>
                <EntityHeaderDecoration 
                urn={urn}
                entityType={entityType}
                displayProperties={displayProperties}
                />
                <EntityBackButton />
                <LeftColumn>
                    {(loading && <EntityTitleLoadingSection />) || (
                        <>
                            <TitleWrapper>
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
                                <EntityName isNameEditable={showEditName} />
                                {entityData?.deprecation?.deprecated && (
                                    <DeprecationPill
                                        urn={urn}
                                        deprecation={entityData?.deprecation}
                                        showUndeprecate
                                        refetch={refetch}
                                    />
                                )}
                                {entityData?.health && <EntityHealth health={entityData.health} baseUrl={entityUrl} />}
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
            {!!subHeader && (
                <SubHeader>
                    <subHeader.component />
                </SubHeader>
            )}
        </>
    );
};
