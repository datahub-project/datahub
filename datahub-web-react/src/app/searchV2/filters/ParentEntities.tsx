import React from 'react';

import BrowsePaths from '@app/previewV2/BrowsePaths';

import { Entity } from '@types';

interface Props {
    parentEntities: Entity[];
    numVisible?: number;
    hideIcons?: boolean;
    linksDisabled?: boolean; // don't allow links to parent entities
}

export default function ParentEntities(props: Props) {
    const { parentEntities, numVisible, hideIcons, linksDisabled } = props;

    const entries = parentEntities && [...parentEntities].reverse().map((entity) => ({ entity }));
    return (
        <BrowsePaths entries={entries} numVisible={numVisible} hideIcons={hideIcons} linksDisabled={linksDisabled} />
    );
}
