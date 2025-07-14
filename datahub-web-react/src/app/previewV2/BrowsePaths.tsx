import { Popover } from '@components';
import React, { useState } from 'react';
import styled from 'styled-components';

import ContextPathEntry from '@app/previewV2/ContextPathEntry';
import ContextPathSeparator from '@app/previewV2/ContextPathSeparator';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Entity, EntityType } from '@types';

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
            content={
                showPopover ? <BrowsePaths {...props} hidePopover numVisible={undefined} linksDisabled={false} /> : null
            }
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
    const entityRegistry = useEntityRegistry();

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

    const { entity } = entry;
    return (
        <>
            <ContextPathEntry
                key={entity?.urn || entry.name}
                name={entity ? entityRegistry.getDisplayName(entity.type, entity) : entry.name}
                linkUrl={entity ? entityRegistry.getEntityUrl(entity.type, entity.urn) : undefined}
                isLast={isLast}
                hideIcon={hideIcons}
                linkDisabled={linksDisabled || hasDataPlatformInstance}
                setIsTruncated={setIsTruncated}
            />
            {!isLast && <ContextPathSeparator />}
        </>
    );
}
