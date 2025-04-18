import React, { useMemo } from 'react';
import styled from 'styled-components';
<<<<<<< HEAD

import { SingleEntityIcon } from '@app/searchV2/autoCompleteV2/components/icon/SingleEntityIcon';
import { EntityIconProps } from '@app/searchV2/autoCompleteV2/components/icon/types';
import useUniqueEntitiesByPlatformUrn from '@app/searchV2/autoCompleteV2/components/icon/useUniqueEntitiesByPlatformUrn';
=======
import { EntityIconProps } from './types';
import { SingleEntityIcon } from './SingleEntityIcon';
import useUniqueEntitiesByPlatformUrn from './useUniqueEntitiesByPlatformUrn';
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105

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
