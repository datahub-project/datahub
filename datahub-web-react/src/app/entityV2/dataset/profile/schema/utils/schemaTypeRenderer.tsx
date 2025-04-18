import { Popover } from '@components';
import React from 'react';
import styled from 'styled-components';
import TypeLabel from '../../../../shared/tabs/Dataset/Schema/components/TypeLabel';
import { ExtendedSchemaFields } from './types';
import { REDESIGN_COLORS } from '../../../../shared/constants';

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
