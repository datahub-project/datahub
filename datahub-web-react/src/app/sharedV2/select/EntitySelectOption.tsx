import { Text } from '@components';
import React, { useCallback } from 'react';
import styled from 'styled-components';

import spacing from '@components/theme/foundations/spacing';

import { isDomain } from '@app/entityV2/domain/utils';
import { isGlossaryTerm } from '@app/entityV2/glossaryTerm/utils';
import { isCorpGroup } from '@app/entityV2/group/utils';
import { isTag } from '@app/entityV2/tag/utils';
import { isCorpUser } from '@app/entityV2/user/utils';
import { DomainSelectOption } from '@app/sharedV2/domains/DomainSelectOption';
import { GlossaryTermSelectOption } from '@app/sharedV2/glossary/GlossaryTermSelectOption';
import { OwnerSelectOption } from '@app/sharedV2/owners/OwnerSelectOption';
import { DefaultEntitySelectOption } from '@app/sharedV2/select/DefaultEntitySelectOption';
import { TagSelectOption } from '@app/sharedV2/tags/TagSelectOption';

import { Entity } from '@types';

const Wrapper = styled.div`
    display: flex;
    gap: ${spacing.xsm};
    align-items: center;
`;

const Counter = styled(Text)`
    color: ${(props) => props.theme.colors.textTertiary};
`;

interface Props {
    entity: Entity;
    counter?: number;
}

export function EntitySelectOption({ entity, counter }: Props) {
    const renderSelectOption = useCallback(() => {
        if (isDomain(entity)) {
            return <DomainSelectOption entity={entity} />;
        }
        if (isTag(entity)) {
            return <TagSelectOption entity={entity} />;
        }
        if (isGlossaryTerm(entity)) {
            return <GlossaryTermSelectOption entity={entity} />;
        }
        if (isCorpUser(entity) || isCorpGroup(entity)) {
            return <OwnerSelectOption entity={entity} />;
        }
        return <DefaultEntitySelectOption entity={entity} />;
    }, [entity]);

    const renderCounter = useCallback(() => {
        if (counter === undefined) return null;

        return <Counter>{counter}</Counter>;
    }, [counter]);

    return (
        <Wrapper>
            {renderSelectOption()}
            {renderCounter()}
        </Wrapper>
    );
}
