import React, { useMemo } from 'react';
import styled from 'styled-components';
import { EntityIconProps } from './types';
import { SingleEntityIcon } from './SingleEntityIcon';
import useUniqueEntitiesByPlatformUrn from './useUniqueEntitiesByPlatformUrn';

const Container = styled.div`
    display: flex;
`;

const ICON_SIZE = 20;
const SIBLING_ICON_SIZE = 16;

export default function DefaultEntityIcon({ entity, siblings }: EntityIconProps) {
    const uniqueSiblingsByPlatform = useUniqueEntitiesByPlatformUrn(siblings);
    const hasSiblings = useMemo(() => (uniqueSiblingsByPlatform?.length ?? 0) > 0, [uniqueSiblingsByPlatform?.length]);
    const entitiesToShowIcons = useMemo(
        () => (hasSiblings ? uniqueSiblingsByPlatform : [entity]),
        [hasSiblings, uniqueSiblingsByPlatform, entity],
    );
    const iconSize = useMemo(() => (hasSiblings ? SIBLING_ICON_SIZE : ICON_SIZE), [hasSiblings]);

    return (
        <Container>
            {entitiesToShowIcons?.map((entityToShowIcon) => (
                <SingleEntityIcon entity={entityToShowIcon} key={entityToShowIcon.urn} size={iconSize} />
            ))}
        </Container>
    );
}
