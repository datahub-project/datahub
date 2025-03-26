import React, { useMemo } from 'react';
import styled from 'styled-components';

import { GenericEntityProperties } from '@src/app/entity/shared/types';
import OverflowList from '@src/app/sharedV2/OverflowList';

import ExternalLink from './ExternalLink';
import ViewMoreDropdown from './components/VeiwMoreDropdown/ViewMoreDropdown';
import { LinkItem } from './types';
import useExternalLinks from './useExternalLinks';
import usePlatrofmLinks from './usePlatformLinks';

const Links = styled.div<{ $shouldTakeAllAvailableSpace?: boolean }>`
    display: flex;
    width: ${(props) => (props.$shouldTakeAllAvailableSpace ? '100%' : 'fit-content')};
    overflow: hidden;
`;

interface Props {
    data: GenericEntityProperties | null;
    className?: string;
    hideSiblingActions?: boolean;
    urn: string;
    suffix?: string;
    shouldFillAllAvailableSpace?: boolean;
}

export default function ViewInPlatform({
    urn,
    className,
    data,
    hideSiblingActions,
    suffix,
    shouldFillAllAvailableSpace = true,
}: Props) {
    const externalLinks = useExternalLinks(urn, data);
    const platformLinks = usePlatrofmLinks(urn, data, !!hideSiblingActions, suffix ?? '', className);

    const linkItems: LinkItem[] = useMemo(() => {
        const links = [...externalLinks, ...platformLinks];

        return links.map((link) => ({
            key: link.url,
            url: link.url,
            description: link.label,
            node: <ExternalLink href={link.url} label={link.label} className={link.className} onClick={link.onClick} />,
            attributes: link,
        }));
    }, [externalLinks, platformLinks]);

    return (
        <Links $shouldTakeAllAvailableSpace={shouldFillAllAvailableSpace}>
            <OverflowList
                items={linkItems}
                renderHiddenItems={(items) => <ViewMoreDropdown linkItems={items} />}
                gap={8}
                shouldFillAllAvailableSpace={shouldFillAllAvailableSpace}
            />
        </Links>
    );
}
