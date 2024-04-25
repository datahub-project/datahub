import React from 'react';
import styled from 'styled-components/macro';
import { Dropdown, Menu } from 'antd';
import { BellFilled, BellOutlined } from '@ant-design/icons';
import useSubscriptionSummary from '../useSubscriptionSummary';
import { ActionMenuItem } from '../../../entityV2/shared/EntityDropdown/styledComponents';
import SubscribeButtonMenu from './SubscribeButtonMenu';
import { GenericEntityProperties } from '../../../entity/shared/types';
import { EntityType } from '../../../../types.generated';

const StyledBellFilled = styled(BellFilled)`
    && {
        height: 100%;
        width: 100%;
        display: flex;
        align-items: center;
        justify-content: center;
    }
`;

const StyledBellOutlined = styled(BellOutlined)`
    && {
        height: 100%;
        width: 100%;
        display: flex;
        align-items: center;
        justify-content: center;
    }
`;

interface Props {
    entityUrn: string;
    entityData: GenericEntityProperties | null;
    entityType: EntityType;
}

export const SubscribeMenuAction = ({ entityUrn, entityData, entityType }: Props) => {
    const { isUserSubscribed, setIsUserSubscribed, refetchSubscriptionSummary } = useSubscriptionSummary({
        entityUrn,
    });

    return (
        <ActionMenuItem key="subscribe">
            <Dropdown
                trigger={['hover']}
                overlay={
                    <Menu>
                        <SubscribeButtonMenu
                            isUserSubscribed={isUserSubscribed}
                            setIsUserSubscribed={setIsUserSubscribed}
                            refetchSubscriptionSummary={refetchSubscriptionSummary}
                            entityUrn={entityUrn}
                            entityData={entityData}
                            entityType={entityType}
                        />
                    </Menu>
                }
            >
                {isUserSubscribed ? <StyledBellFilled /> : <StyledBellOutlined />}
            </Dropdown>
        </ActionMenuItem>
    );
};
