import React, { useState } from 'react';
import { ReferenceSectionProps } from '../../types';
import { ReferenceSection } from '../../../layout/shared/styledComponents';
import { useGetPinnedLinks } from './useGetPinnedLinks';
import { PinnedLinkList } from './PinnedLinkList';

const DEFAULT_MAX_LINKS_TO_SHOW = 3;

export const PinnedLinks = ({ hideIfEmpty }: ReferenceSectionProps) => {
    const [linkCount, setLinkCount] = useState(DEFAULT_MAX_LINKS_TO_SHOW);
    const { links } = useGetPinnedLinks();

    if (hideIfEmpty && links.length === 0) {
        return null;
    }

    return (
        <ReferenceSection>
            <PinnedLinkList
                links={links.slice(0, linkCount)}
                title="Pinned links"
                tip="Pinned links for your organization"
                showMore={links.length > linkCount}
                onClickMore={() => setLinkCount(linkCount + DEFAULT_MAX_LINKS_TO_SHOW)}
                showMoreCount={
                    linkCount + DEFAULT_MAX_LINKS_TO_SHOW > links.length
                        ? links.length - linkCount
                        : DEFAULT_MAX_LINKS_TO_SHOW
                }
            />
        </ReferenceSection>
    );
};
