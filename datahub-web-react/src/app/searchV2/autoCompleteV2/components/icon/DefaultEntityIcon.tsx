import { colors, radius } from '@components';
import React, { useMemo } from 'react';
import styled from 'styled-components';

import { getEntityPlatforms } from '@app/entityV2/shared/containers/profile/header/utils';
import { PlatformIcon } from '@app/searchV2/autoCompleteV2/components/icon/PlatformIcon';
import { SingleEntityIcon } from '@app/searchV2/autoCompleteV2/components/icon/SingleEntityIcon';
import { EntityIconProps } from '@app/searchV2/autoCompleteV2/components/icon/types';
import useUniqueEntitiesByPlatformUrn from '@app/searchV2/autoCompleteV2/components/icon/useUniqueEntitiesByPlatformUrn';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

const Container = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
    background: ${colors.gray[1500]};
    height: 28px;
    width: 28px;
    border-radius: ${radius.full};
`;

const IconContainer = styled.div`
    margin-left: -4px;
    &:first-child {
        margin-left: 0;
    }
`;

const ICON_SIZE = 20;
const SIBLING_ICON_SIZE = 16;

export default function DefaultEntityIcon({ entity, siblings }: EntityIconProps) {
    const entityRegistry = useEntityRegistryV2();
    const uniqueSiblingsByPlatform = useUniqueEntitiesByPlatformUrn(siblings);
    const hasSiblings = useMemo(() => (uniqueSiblingsByPlatform?.length ?? 0) > 0, [uniqueSiblingsByPlatform?.length]);
    const entitiesToShowIcons = useMemo(
        () => (hasSiblings ? uniqueSiblingsByPlatform : [entity]),
        [hasSiblings, uniqueSiblingsByPlatform, entity],
    );
    const iconSize = useMemo(() => (hasSiblings ? SIBLING_ICON_SIZE : ICON_SIZE), [hasSiblings]);

    const properties = entityRegistry.getGenericEntityProperties(entity.type, entity);
    const { platforms } = getEntityPlatforms(entity.type, properties);

    if (!hasSiblings && (platforms?.length ?? 0) > 1) {
        return (
            <Container>
                {platforms?.map((platform) => (
                    <IconContainer>
                        <PlatformIcon platform={platform} size={SIBLING_ICON_SIZE} />
                    </IconContainer>
                ))}
            </Container>
        );
    }

    return (
        <Container>
            {entitiesToShowIcons?.map((entityToShowIcon) => (
                <IconContainer>
                    <SingleEntityIcon entity={entityToShowIcon} key={entityToShowIcon.urn} size={iconSize} />
                </IconContainer>
            ))}
        </Container>
    );
}
