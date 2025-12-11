/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import Icon from '@ant-design/icons/lib/components/Icon';
import React, { useMemo } from 'react';
import styled from 'styled-components';

import translateFieldPath from '@app/entity/dataset/profile/schema/utils/translateFieldPath';
import { useEntityData } from '@app/entity/shared/EntityContext';
import { ANTD_GRAY_V2 } from '@app/entity/shared/constants';
import { getNumPromptsCompletedForField } from '@app/entity/shared/containers/profile/sidebar/FormInfo/utils';
import { useEntityFormContext } from '@app/entity/shared/entityForm/EntityFormContext';
import { pluralize } from '@app/shared/textUtil';

import { SchemaField } from '@types';

import GreenCircleIcon from '@images/greenCircleTwoTone.svg?react';

const HeaderWrapper = styled.div`
    display: flex;
    justify-content: space-between;
    font-size: 16px;
    align-items: center;
`;

const PromptsRemainingText = styled.span`
    font-size: 14px;
    color: ${ANTD_GRAY_V2[8]};
    font-weight: 400;
`;

const PromptsCompletedText = styled.span`
    font-size: 14px;
    color: #373d44;
    font-weight: 600;
`;

interface Props {
    field: SchemaField;
    numPrompts: number;
    isExpanded: boolean;
}

export default function DropdownHeader({ field, numPrompts, isExpanded }: Props) {
    const { entityData } = useEntityData();
    const { formUrn } = useEntityFormContext();
    const numPromptsCompletedForField = useMemo(
        () => getNumPromptsCompletedForField(field.fieldPath, entityData, formUrn),
        [entityData, field.fieldPath, formUrn],
    );
    const numPromptsRemaining = numPrompts - numPromptsCompletedForField;

    return (
        <HeaderWrapper>
            <span>Field: {translateFieldPath(field.fieldPath)}</span>
            {numPromptsRemaining > 0 && (
                <PromptsRemainingText>
                    {numPromptsRemaining} {pluralize(numPrompts, 'question')} remaining
                </PromptsRemainingText>
            )}
            {numPromptsRemaining === 0 && !isExpanded && (
                <PromptsCompletedText>
                    <Icon component={GreenCircleIcon} /> {numPrompts} {pluralize(numPrompts, 'Question')} Completed
                </PromptsCompletedText>
            )}
        </HeaderWrapper>
    );
}
