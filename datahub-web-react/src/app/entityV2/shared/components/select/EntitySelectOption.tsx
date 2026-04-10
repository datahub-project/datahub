import { Pill } from '@components';
import React, { useCallback } from 'react';
import styled from 'styled-components';

import spacing from '@components/theme/foundations/spacing';

import { isDomain } from '@app/entityV2/domain/utils';
import { GlossaryTermSelectOption } from '@app/entityV2/glossaryTerm/components/GlossaryTermSelectOption';
import { isGlossaryTerm } from '@app/entityV2/glossaryTerm/utils';
import { isCorpGroup } from '@app/entityV2/group/utils';
import { DefaultEntitySelectOption } from '@app/entityV2/shared/components/select/DefaultEntitySelectOption';
import { getParentEntities } from '@app/entityV2/shared/utils/getParentEntities';
import { isTag } from '@app/entityV2/tag/utils';
import { isCorpUser } from '@app/entityV2/user/utils';
import ParentEntities from '@app/searchV2/filters/ParentEntities';
import { getCounterText } from '@app/searchV2/filters/utils';
import { DomainSelectOption } from '@app/sharedV2/domains/DomainSelectOption';
import { OwnerSelectOption } from '@app/sharedV2/owners/OwnerSelectOption';
import { TagSelectOption } from '@app/sharedV2/tags/TagSelectOption';

import { Entity } from '@types';

const Wrapper = styled.div`
    display: flex;
    gap: ${spacing.xsm};
    align-items: center;
    width: 100%;
    justify-content: space-between;
`;

const Column = styled.div`
    display: flex;
    flex-direction: column;
`;

const ParentWrapper = styled.div`
    max-width: 220px;
    font-size: 12px;
`;

interface Props {
    entity: Entity;
    counter?: number;
    showParentEntityPath?: boolean;
}

export function EntitySelectOption({ entity, counter, showParentEntityPath }: Props) {
    const parentEntities: Entity[] = getParentEntities(entity) || [];

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

        return <Pill label={getCounterText(counter)} size="xs" variant="filled" color="gray" />;
    }, [counter]);

    return (
        <Wrapper>
            <Column>
                {showParentEntityPath && (
                    <ParentWrapper>
                        <ParentEntities parentEntities={parentEntities} />
                    </ParentWrapper>
                )}
                {renderSelectOption()}
            </Column>
            {renderCounter()}
        </Wrapper>
    );
}
