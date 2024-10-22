import ViewInPlatform from '@app/entityV2/shared/externalUrl/ViewInPlatform';
import React from 'react';
import styled from 'styled-components';
import { Maybe } from 'graphql/jsutils/Maybe';
import ColoredBackgroundPlatformIconGroup, { PlatformContentWrapper } from './ColoredBackgroundPlatformIconGroup';
import MoreOptionsMenuAction from '../entityV2/shared/EntityDropdown/MoreOptionsMenuAction';
import EntityHeader, { StyledLink } from './EntityHeader';
import { PreviewType } from '../entity/Entity';
import { BrowsePathV2, Dataset, Deprecation, Entity, EntityType, Health } from '../../types.generated';
import { EntityMenuActions } from '../entityV2/Entity';
import { EntityMenuItems } from '../entityV2/shared/EntityDropdown/EntityMenuActions';
import { GenericEntityProperties } from '../entity/shared/types';
import ContextPath from './ContextPath';
import { REDESIGN_COLORS } from '../entityV2/shared/constants';

interface RowContainerProps {
    hidden?: boolean;
    alignment?: 'flex-start' | 'center' | 'flex-end' | 'self-start';
}

export const RowContainer = styled.div<RowContainerProps>`
    align-items: ${(props) => props.alignment || 'center'};
    display: ${(props) => (props.hidden ? 'none' : 'flex')};
    flex-direction: row;
    width: 100%;
    justify-content: space-between;

    ${PlatformContentWrapper} {
        max-width: fit-content;
        margin: 0px;
        margin-right: 5px;
    }
`;
export const ActionsAndStatusSection = styled.div`
    display: flex;
    flex-direction: row;
    justify-content: end;
    margin-right: -0.3rem;
    flex: 0 0 auto;
`;

export const ActionsSection = styled.div`
    display: flex;
    flex-direction: row;
    gap: 5px;
`;

export const HeaderContainer = styled.div`
    display: flex;
    flex-direction: row;
    align-items: center;
    width: 100%;
`;

const HeaderContainerV2 = styled.div`
    display: flex;
    flex-direction: column;
    width: 70%;
    flex: 1 1 auto;
    ${StyledLink} {
        max-width: 75%;
    }
`;
const PlatformDivider = styled.div`
    font-size: 16px;
    margin-right: 0.5rem;
    margin-top: -3px;
    color: ${REDESIGN_COLORS.TEXT_GREY};
`;

interface Props {
    name: string;
    urn: string;
    isIconPresent: boolean;
    url: string;
    entityType: EntityType;
    searchEntity: Dataset | undefined | null;
    platform?: string;
    platforms?: Maybe<string | undefined>[];
    deprecation?: Deprecation | null;
    titleSizePx?: number;
    onClick?: () => void;
    // this is provided by the impact analysis view. it is used to display
    // how the listed node is connected to the source node
    degree?: number;
    previewType?: Maybe<PreviewType>;
    health?: Health[];
    // eslint-disable-next-line react/no-unused-prop-types
    description?: string;
    // eslint-disable-next-line react/no-unused-prop-types
    qualifier?: string | null;
    // eslint-disable-next-line react/no-unused-prop-types
    externalUrl?: string | null;
    isOutputPort?: boolean;
    headerDropdownItems?: Set<EntityMenuItems>;
    actions?: EntityMenuActions;
    entityIcon?: JSX.Element;
    logoUrls?: Maybe<string | undefined>[];
    logoUrl?: string;
    previewData: GenericEntityProperties | null;
    platformInstanceId?: string;
    typeIcon?: JSX.Element;
    finalType?: string | undefined;
    parentEntities?: Entity[] | null;
    contentRef: React.RefObject<HTMLDivElement>;
    browsePaths?: BrowsePathV2 | undefined;
}

export const CompactView = ({
    name,
    onClick,
    titleSizePx,
    url,
    degree,
    deprecation,
    actions,
    health,
    previewData,
    isIconPresent,
    platform,
    logoUrl,
    platforms,
    logoUrls,
    isOutputPort,
    entityIcon,
    headerDropdownItems,
    previewType,
    urn,
    entityType,
    searchEntity,
    platformInstanceId,
    typeIcon,
    finalType,
    parentEntities, // eslint-disable-next-line @typescript-eslint/no-unused-vars
    contentRef,
    browsePaths,
}: Props) => {
    return (
        <>
            <RowContainer alignment="flex-start">
                <HeaderContainerV2>
                    <EntityHeader
                        name={name}
                        onClick={onClick}
                        previewType={previewType}
                        titleSizePx={titleSizePx}
                        url={url}
                        urn={urn}
                        deprecation={deprecation}
                        health={health}
                        degree={degree}
                        connectionName={previewData?.name}
                    />
                    <div style={{ display: 'flex', marginTop: '0.2rem' }}>
                        {isIconPresent ? (
                            <div style={{ display: 'flex', alignItems: 'center' }}>
                                <ColoredBackgroundPlatformIconGroup
                                    platformName={platform}
                                    platformLogoUrl={logoUrl}
                                    platformNames={platforms}
                                    platformLogoUrls={logoUrls}
                                    isOutputPort={isOutputPort}
                                    icon={entityIcon}
                                    imgSize={10}
                                    backgroundSize={20}
                                />
                                <PlatformDivider> | </PlatformDivider>
                            </div>
                        ) : (
                            <div />
                        )}
                        <ContextPath
                            instanceId={platformInstanceId}
                            typeIcon={typeIcon}
                            type={finalType}
                            entityType={entityType}
                            browsePaths={browsePaths}
                            parentEntities={parentEntities}
                            contentRef={contentRef}
                            entityTitleWidth={previewType === PreviewType.HOVER_CARD ? 150 : 200}
                            previewType={previewType}
                            isCompactView
                        />
                    </div>
                </HeaderContainerV2>
                <ActionsAndStatusSection>
                    <ActionsSection>
                        {headerDropdownItems && previewType !== PreviewType.HOVER_CARD && (
                            <MoreOptionsMenuAction
                                menuItems={headerDropdownItems}
                                urn={urn}
                                entityType={entityType}
                                entityData={previewData}
                                triggerType={['click']}
                                actions={actions}
                            />
                        )}
                        <ViewInPlatform searchEntity={searchEntity} urn={urn} />
                    </ActionsSection>
                </ActionsAndStatusSection>
            </RowContainer>
        </>
    );
};
