import React, { useMemo } from 'react';
import styled from 'styled-components';

import ExternalLink from '@app/entityV2/shared/externalUrl/ExternalLink';
import ViewMoreDropdown from '@app/entityV2/shared/externalUrl/components/VeiwMoreDropdown/ViewMoreDropdown';
import { LinkItem } from '@app/entityV2/shared/externalUrl/types';
import useExternalLinks from '@app/entityV2/shared/externalUrl/useExternalLinks';
import usePlatrofmLinks from '@app/entityV2/shared/externalUrl/usePlatformLinks';
import { GenericEntityProperties } from '@src/app/entity/shared/types';
import OverflowList from '@src/app/sharedV2/OverflowList';

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
    isEntityPageHeader?: boolean;
}

export default function ViewInPlatform({
    urn,
    className,
    data,
    hideSiblingActions,
    suffix,
    shouldFillAllAvailableSpace = true,
    isEntityPageHeader = false,
}: Props) {
    const externalLinks = useExternalLinks(urn, data);
    const platformLinks = usePlatrofmLinks(urn, data, hideSiblingActions, suffix ?? '', className);

    const linkItems: LinkItem[] = useMemo(() => {
        const links = [...externalLinks, ...platformLinks];

        return links.map((link) => ({
            key: `${link.url}-${link.label}`,
            url: link.url,
            description: link.label,
            node: (
                <ExternalLink
                    href={link.url}
                    label={link.label}
                    className={link.className}
                    onClick={link.onClick}
                    isEntityPageHeader={isEntityPageHeader}
                />
            ),
            attributes: link,
        }));
    }, [externalLinks, isEntityPageHeader, platformLinks]);

    if (linkItems.length === 0) return null;

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
