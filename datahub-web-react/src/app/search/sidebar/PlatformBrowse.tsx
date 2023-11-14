import React from 'react';
import { BrowseProvider } from './BrowseContext';
import SidebarLoadingError from './SidebarLoadingError';
import useSidebarPlatforms from './useSidebarPlatforms';
import PlatformNode from './PlatformNode';

type Props = {
    visible: boolean;
};

const PlatformBrowse = ({ visible }: Props) => {
    const { error, platformAggregations, retry } = useSidebarPlatforms({
        skip: !visible,
    });

    return (
        <>
            {platformAggregations && !platformAggregations.length && <div>No results found</div>}
            {platformAggregations?.map((platformAggregation) => (
                <BrowseProvider key={platformAggregation.value} platformAggregation={platformAggregation}>
                    <PlatformNode />
                </BrowseProvider>
            ))}
            {error && <SidebarLoadingError onClickRetry={retry} />}
        </>
    );
};

export default PlatformBrowse;
