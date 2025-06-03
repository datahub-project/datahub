import { Popover, colors } from '@components';
import React, { useState } from 'react';
import styled from 'styled-components';

import ContextPathEntityLink from '@app/previewV2/ContextPathEntityLink';
import ContextPathSeparator from '@app/previewV2/ContextPathSeparator';

import { Entity, EntityType } from '@types';

const PlatFormTitle = styled.span<{ isLast: boolean }>`
    flex: ${({ isLast }) => (isLast ? '1 0 1' : '1 1 0')};
    max-width: max-content;

    display: inline-block;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    color: ${colors.gray[1700]};
`;

const ParentNodesWrapper = styled.div`
    align-items: center;
    white-space: nowrap;
    text-overflow: ellipsis;
    display: flex;
    overflow: hidden;
`;

interface Entry {
    name?: string;
    entity?: Entity | null;
}

interface Props {
    entries: Entry[];
    hideIcons?: boolean;
    linksDisabled?: boolean;
    numVisible?: number;
    hidePopover?: boolean;
}

export default function BrowsePaths(props: Props) {
    const { entries, hideIcons, linksDisabled, numVisible, hidePopover } = props;

    const [areChildrenTruncated, setAreChildrenTruncated] = useState({});
    const showEllipsis = !!(numVisible && entries.length > numVisible);
    const showPopover = (Object.values(areChildrenTruncated).includes(true) || showEllipsis) && !hidePopover;
    const truncatedEntries = entries.slice(numVisible ? entries.length - numVisible : 0);

    if (!truncatedEntries?.length) return null;
    return (
        <Popover
            zIndex={1051} // .ant-select-dropdown is 1050
            content={showPopover ? <BrowsePaths {...props} hidePopover numVisible={undefined} /> : null}
            overlayStyle={linksDisabled ? { pointerEvents: 'none' } : {}}
        >
            <ParentNodesWrapper>
                {showEllipsis && (
                    <>
                        <span>...</span>
                        <ContextPathSeparator />
                    </>
                )}
                {truncatedEntries?.map((entry, index) => (
                    <SingleBrowsePath
                        key={index} // eslint-disable-line react/no-array-index-key -- order should not change
                        entry={entry}
                        index={index}
                        isLast={index === truncatedEntries.length - 1}
                        setAreChildrenTruncated={setAreChildrenTruncated}
                        hideIcons={hideIcons}
                        linksDisabled={linksDisabled}
                    />
                ))}
            </ParentNodesWrapper>
        </Popover>
    );
}

interface SingleBrowsePathsProps extends Pick<Props, 'hideIcons' | 'linksDisabled'> {
    entry: Entry;
    index: number;
    isLast: boolean;
    setAreChildrenTruncated: React.Dispatch<React.SetStateAction<Record<number, boolean>>>;
}

function SingleBrowsePath({
    entry,
    index,
    isLast,
    setAreChildrenTruncated,
    hideIcons,
    linksDisabled,
}: SingleBrowsePathsProps) {
    const hasDataPlatformInstance =
        entry.name?.includes('dataPlatformInstance') || entry.entity?.type === EntityType.DataPlatformInstance;
    const setIsTruncated = React.useCallback(
        (v: boolean) =>
            setAreChildrenTruncated((prev) => ({
                ...prev,
                [index]: v,
            })),
        [index, setAreChildrenTruncated],
    );

    return (
        <>
            {!entry.entity && <PlatFormTitle isLast={isLast}>{entry.name}</PlatFormTitle>}
            <ContextPathEntityLink
                key={entry?.entity?.urn}
                entity={entry?.entity}
                isLast={isLast}
                hideIcon={hideIcons}
                linkDisabled={linksDisabled || hasDataPlatformInstance}
                setIsTruncated={setIsTruncated}
            />
            {!isLast && <ContextPathSeparator />}
        </>
    );
}
