import React from 'react';
import styled from 'styled-components';
import LaunchIcon from '@mui/icons-material/Launch';
import { Maybe } from 'graphql/jsutils/Maybe';
import ColoredBackgroundPlatformIconGroup, { PlatformContentWrapper } from './ColoredBackgroundPlatformIconGroup';
import MoreOptionsMenuAction from '../entityV2/shared/EntityDropdown/MoreOptionsMenuAction';
import EntityHeader from './EntityHeader';
import { PreviewType } from '../entity/Entity';
import { BrowsePathV2, Deprecation, Entity, EntityType, Health, ParentContainersResult } from '../../types.generated';
import { EntityMenuActions } from '../entityV2/Entity';
import { EntityMenuItems } from '../entityV2/shared/EntityDropdown/EntityMenuActions';
import { GenericEntityProperties } from '../entity/shared/types';
import SearchCardBrowsePath from './SearchCardBrowsePath';
import StaticSearchCardBrowsePath from './StaticSearchCardBrowsePath';
import { REDESIGN_COLORS } from '../entityV2/shared/constants';
import StyledExternalLink from '../entityV2/shared/links/StyledExternalLink';

interface RowContainerProps {
    hidden?: boolean;
    alignment?: 'flex-start' | 'center' | 'flex-end' | 'self-start';
}

export const RowContainer = styled.div<RowContainerProps>`
    align-items: ${(props) => props.alignment || 'center'};
    display: ${(props) => (props.hidden ? 'none' : 'flex')};
    flex-direction: row;
    width: 100%;
    gap: 5px;

    ${PlatformContentWrapper} {
        max-width: fit-content;
        margin: 0px;
        margin-right: 5px;
    }
`;
export const ActionsAndStatusSection = styled.div`
    display: flex;
    flex-direction: row;
    gap: 5px;
    width: 100%;
    justify-content: end;
    margin-right: -0.3rem;
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
    align-items: start;
    max-width: 55%;
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
    hasParentContainers?: boolean | null | undefined;
    platformInstanceId?: string;
    typeIcon?: JSX.Element;
    finalType?: string | undefined;
    parentEntities?: Entity[] | null;
    parentContainers?: ParentContainersResult | null;
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
    externalUrl,
    headerDropdownItems,
    previewType,
    urn,
    entityType,
    hasParentContainers,
    platformInstanceId,
    typeIcon,
    finalType,
    parentEntities,
    parentContainers,
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
                        {hasParentContainers && (
                            <SearchCardBrowsePath
                                instanceId={platformInstanceId}
                                typeIcon={typeIcon}
                                type={finalType}
                                entityType={entityType}
                                parentContainers={parentContainers?.containers}
                                parentEntities={parentEntities}
                                parentContainersRef={contentRef}
                                areContainersTruncated={false}
                                entityTitleWidth={previewType === PreviewType.HOVER_CARD ? 150 : 200}
                                previewType={previewType}
                                isCompactView
                            />
                        )}
                        {!hasParentContainers && (
                            <StaticSearchCardBrowsePath
                                entityType={entityType}
                                browsePaths={browsePaths}
                                type={finalType}
                                isCompactView
                            />
                        )}
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
                        {externalUrl && (
                            <StyledExternalLink url={externalUrl}>
                                <LaunchIcon
                                    style={{
                                        fontSize: '16px',
                                    }}
                                />
                                View in {platform}
                            </StyledExternalLink>
                        )}
                    </ActionsSection>
                </ActionsAndStatusSection>
            </RowContainer>
        </>
    );
};
