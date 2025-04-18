import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { EntityStagedForAssertion } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/types';
import { CreateAssertionButton } from '@app/entityV2/shared/tabs/Dataset/Validations/CreateAssertionButton';
import { EntityPrivileges } from '@src/types.generated';

const AssertionTitleContainer = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin: 20px;
    div {
        border-bottom: 0px;
    }
`;
const AssertionListTitle = styled(Typography.Title)`
    && {
        margin-bottom: 0px;
    }
`;

const SubTitle = styled(Typography.Text)`
    font-size: 14px;
    color: #5f6685;
`;

export const AssertionListTitleContainer = ({
    privileges,
    onCreateAssertion,
}: {
    privileges: EntityPrivileges;
    onCreateAssertion: (params: EntityStagedForAssertion) => void;
}) => {
    return (
        <AssertionTitleContainer>
            <div className="left-section">
                <AssertionListTitle level={4}>Assertions</AssertionListTitle>
                <SubTitle>View and manage data quality checks for this table</SubTitle>
            </div>
            <CreateAssertionButton privileges={privileges} onCreateAssertion={onCreateAssertion} />
        </AssertionTitleContainer>
    );
};
