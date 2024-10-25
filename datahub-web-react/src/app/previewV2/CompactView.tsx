import ViewInPlatform from '@app/entityV2/shared/externalUrl/ViewInPlatform';
import { ActionsAndStatusSection } from '@app/previewV2/shared';
import React from 'react';
import styled from 'styled-components';
import { Maybe } from 'graphql/jsutils/Maybe';
import ColoredBackgroundPlatformIconGroup, { PlatformContentWrapper } from './ColoredBackgroundPlatformIconGroup';
import MoreOptionsMenuAction from '../entityV2/shared/EntityDropdown/MoreOptionsMenuAction';
import EntityHeader from './EntityHeader';
import { PreviewType } from '../entity/Entity';
import { BrowsePathV2, Deprecation, Entity, EntityType, Health } from '../../types.generated';
import { EntityMenuActions } from '../entityV2/Entity';
import { EntityMenuItems } from '../entityV2/shared/EntityDropdown/EntityMenuActions';
import { GenericEntityProperties } from '../entity/shared/types';
import ContextPath from './ContextPath';
import { REDESIGN_COLORS } from '../entityV2/shared/constants';

const RowContainer = styled.div`
    display: flex;
    flex-direction: row;
    width: 100%;
    align-items: self-start;
    justify-content: space-between;

    ${PlatformContentWrapper} {
        max-width: fit-content;
        margin: 0px;
        margin-right: 5px;
    }
`;

const CompactActionsAndStatusSection = styled(ActionsAndStatusSection)`
    justify-content: end;
    margin-right: -0.3rem;
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
    data: GenericEntityProperties | null;
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
    data,
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
    platformInstanceId,
    typeIcon,
    finalType,
    parentEntities, // eslint-disable-next-line @typescript-eslint/no-unused-vars
    contentRef,
    browsePaths,
}: Props) => {
    return (
        <>
            <RowContainer>
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
                <CompactActionsAndStatusSection>
                    <ViewInPlatform data={data} urn={urn} />
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
                </CompactActionsAndStatusSection>
            </RowContainer>
            <RowContainer>
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
            </RowContainer>
        </>
    );
};
