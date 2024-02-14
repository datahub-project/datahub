import React from 'react';
import styled from 'styled-components/macro';
import EntityMenuActions, { EntityMenuItems } from '../../../EntityDropdown/EntityMenuActions';
import PlatformContent from './PlatformContent';
import { EntityHealth } from './EntityHealth';
import EntityName from './EntityName';
import { DeprecationPill } from '../../../components/styled/DeprecationPill';
import EntityActions, { EntityActionItem } from '../../../entity/EntityActions';
import EntityTitleLoadingSection from './EntityHeaderLoadingSection';
import EntityPlatformLoadingSection from './EntityPlatformLoadingSection';

const TitleWrapper = styled.div`
    display: flex;
    justify-content: start;
    align-items: center;
    padding: 0px 0px 0px 0px;
    .ant-typography-edit-content {
        padding-top: 7px;
        margin-left: 15px;
    }
`;

const Row = styled.div`
    padding: 8px 16px;
    display: flex;
    flex-direction: row;
    align-items: space-between;
`;

const PlatformRow = styled(Row)`
    padding: 0px 16px 4px 16px;
`;

const LeftColumn = styled.div`
    flex: 1;
    width: 70%;
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: left;
`;

const RightColumn = styled.div`
    display: flex;
    flex-direction: column;
    align-items: end;
    justify-content: center;
`;

const TopButtonsWrapper = styled.div`
    display: flex;
    justify-content: flex-end;
    gap: 8px;
`;

type Props = {
    urn: string;
    loading: boolean;
    entityData?: any;
    entityUrl: string;
    refetch: () => void;
    headerDropdownItems?: Set<EntityMenuItems>;
    headerActionItems?: Set<EntityActionItem>;
    showEditName?: boolean;
};

export const CompactEntityHeader = ({
    urn,
    loading,
    entityUrl,
    entityData,
    refetch,
    headerActionItems,
    headerDropdownItems,
    showEditName,
}: Props) => {
    return (
        <>
            <Row>
                <LeftColumn>
                    {(loading && <EntityTitleLoadingSection />) || (
                        <>
                            <TitleWrapper>
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
            <PlatformRow>
                <LeftColumn>{(loading && <EntityPlatformLoadingSection />) || <PlatformContent />}</LeftColumn>
            </PlatformRow>
        </>
    );
};
