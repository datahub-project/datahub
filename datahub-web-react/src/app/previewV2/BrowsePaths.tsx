import { Maybe } from 'graphql/jsutils/Maybe';
import React from 'react';
import styled from 'styled-components';

import { PreviewType } from '@app/entityV2/Entity';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import {
    Ellipsis,
    StyledTooltip,
} from '@app/entityV2/shared/containers/profile/header/PlatformContent/ParentNodesView';
import ContextPathEntityLink from '@app/previewV2/ContextPathEntityLink';
import { ContextPathSeparator } from '@app/previewV2/ContextPathSeparator';

import { BrowsePathEntry, BrowsePathV2 } from '@types';

export const PlatformText = styled.div<{
    $maxWidth?: number;
    $previewType?: Maybe<PreviewType>;
    $isCompactView?: boolean;
    $color?: string;
}>`
    color: ${(props) => props.$color ?? REDESIGN_COLORS.TEXT_GREY};
    white-space: nowrap;
    font-family: Mulish;
    font-style: normal;
    font-weight: 500;
    text-overflow: ellipsis;
    overflow: hidden;
    display: flex;
    align-items: center;
    ${(props) => (props.$isCompactView ? '12px' : '13px')}
    ${(props) => props.$maxWidth && `max-width: ${props.$maxWidth}px;`}
`;

export function getParentBrowsePathNames(browsePaths?: Maybe<BrowsePathEntry>[] | null) {
    let parentNames = '';
    if (browsePaths) {
        [...browsePaths].reverse().forEach((path, index) => {
            if (path?.name) {
                if (index !== 0) {
                    parentNames += ' > ';
                }
                parentNames += path.name;
            }
        });
    }
    return parentNames;
}

const PlatFormTitle = styled.span<{ $color?: string }>`
    display: inline-block;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    color: ${(props) => props.$color ?? REDESIGN_COLORS.TEXT_GREY};
`;

const ParentNodesWrapper = styled.div`
    align-items: center;
    white-space: nowrap;
    text-overflow: ellipsis;
    display: flex;
    max-width: 460px;
    text-overflow: ellipsis;
    overflow: hidden;
`;

interface Props {
    previewType?: Maybe<PreviewType>;
    browsePaths?: Maybe<BrowsePathV2> | undefined;
    contentRef: React.RefObject<HTMLDivElement>;
    isContentTruncated: boolean;
    linksDisabled?: boolean;
    color?: string;
}

const BrowsePathSection = ({
    path,
    linksDisabled,
    color,
}: { path: BrowsePathEntry } & Pick<Props, 'linksDisabled' | 'color'>) => {
    if (!path.entity) {
        return <PlatFormTitle $color={color}>{path.name}</PlatFormTitle>;
    }

    // Till we have a DataPlatform instance page
    const hasDataPlatformInstance = path.name?.includes('dataPlatformInstance');

    return (
        <ContextPathEntityLink
            key={path?.entity?.urn}
            entity={path?.entity}
            style={{ fontSize: '12px' }}
            linkDisabled={linksDisabled || hasDataPlatformInstance}
        />
    );
};

function BrowsePaths(props: Props) {
    const { previewType, browsePaths, contentRef, isContentTruncated, linksDisabled, color } = props;

    const parentPath = browsePaths?.path?.[0];
    const remainingParentPaths = browsePaths?.path && browsePaths.path.slice(1, browsePaths.path.length);

    return (
        <StyledTooltip
            title={getParentBrowsePathNames(browsePaths?.path)}
            overlayStyle={isContentTruncated ? {} : { display: 'none' }}
            maxWidth={previewType === PreviewType.HOVER_CARD ? 300 : 620}
        >
            {isContentTruncated && <Ellipsis $color={color}>...</Ellipsis>}
            {/* To avoid rendering a empty div */}
            {(parentPath || remainingParentPaths) && (
                <ParentNodesWrapper ref={contentRef}>
                    {parentPath && (
                        <PlatformText $color={color}>
                            <BrowsePathSection color={color} path={parentPath} linksDisabled={linksDisabled} />
                            {remainingParentPaths && remainingParentPaths?.length > 0 && (
                                <ContextPathSeparator $color={color} />
                            )}
                        </PlatformText>
                    )}
                    {remainingParentPaths &&
                        remainingParentPaths.map((container, index) => {
                            return (
                                <PlatformText $color={color}>
                                    <BrowsePathSection color={color} path={container} linksDisabled={linksDisabled} />
                                    {index < remainingParentPaths.length - 1 && <ContextPathSeparator $color={color} />}
                                </PlatformText>
                            );
                        })}
                </ParentNodesWrapper>
            )}
        </StyledTooltip>
    );
}

export default BrowsePaths;
