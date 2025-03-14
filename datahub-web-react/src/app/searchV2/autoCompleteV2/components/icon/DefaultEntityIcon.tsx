import React, { useMemo } from 'react';
import styled from 'styled-components';
import { EntityIconProps } from './types';
import { SingleEntityIcon } from './SingleEntityIcon';

const Container = styled.div`
    display: flex;
`;

const ICON_SIZE = 20;
const SIBLING_ICON_SIZE = 16;

export default function DefaultEntityIcon({ entity, siblings }: EntityIconProps) {
    const hasSiblings = useMemo(() => (siblings?.length ?? 0) > 0, [siblings?.length]);
    const entitiesToShowIcons = useMemo(() => (hasSiblings ? siblings : [entity]), [hasSiblings, siblings, entity]);
    const iconSize = useMemo(() => (hasSiblings ? SIBLING_ICON_SIZE : ICON_SIZE), [hasSiblings]);

    return (
        <Container>
            {entitiesToShowIcons?.map((entityToShowIcon) => (
                <SingleEntityIcon entity={entityToShowIcon} key={entityToShowIcon.urn} size={iconSize} />
            ))}
        </Container>
    );
}
