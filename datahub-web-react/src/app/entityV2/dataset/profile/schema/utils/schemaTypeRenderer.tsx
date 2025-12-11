/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Popover } from '@components';
import React from 'react';
import styled from 'styled-components';

import { ExtendedSchemaFields } from '@app/entityV2/dataset/profile/schema/utils/types';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import TypeLabel from '@app/entityV2/shared/tabs/Dataset/Schema/components/TypeLabel';

const FieldTypeWrapper = styled.div`
    display: inline-flex;
    align-items: center;
`;

const FieldTypeContainer = styled.div`
    vertical-align: top;
    display: flex;
    color: ${REDESIGN_COLORS.GREY_500};
`;

type InteriorTypeProps = {
    record: ExtendedSchemaFields;
};

const InteriorTypeContent = ({ record }: InteriorTypeProps) => {
    return (
        <FieldTypeWrapper>
            <FieldTypeContainer>
                <TypeLabel type={record.type} nativeDataType={record.nativeDataType} />
            </FieldTypeContainer>
        </FieldTypeWrapper>
    );
};

export default function useSchemaTypeRenderer() {
    return (fieldPath: string, record: ExtendedSchemaFields): JSX.Element => {
        return (
            <Popover content={<InteriorTypeContent record={record} />}>
                <InteriorTypeContent record={record} />
            </Popover>
        );
    };
}
