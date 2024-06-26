import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { Dropdown, Menu } from 'antd';
import { BellFilled, BellOutlined } from '@ant-design/icons';
import { ActionMenuItem } from '../../../entityV2/shared/EntityDropdown/styledComponents';
import SubscribeButtonMenu from './SubscribeButtonMenu';
import { GenericEntityProperties } from '../../../entity/shared/types';
import { EntityType } from '../../../../types.generated';
import Loading from '../../Loading';

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
    const [isSubscribed, setIsSubscribed] = useState(false);
    const [isFetchingSubscriptionSummary, setIsFetchingSubscriptionSummary] = useState(false);

    const renderSubscribeIcon = () => {
        if (isFetchingSubscriptionSummary) {
            return <Loading height={13} />;
        }
        if (isSubscribed) {
            return <StyledBellFilled />;
        }
        return <StyledBellOutlined />;
    };

    return (
        <ActionMenuItem key="subscribe">
            <Dropdown
                trigger={['hover']}
                overlay={
                    <Menu>
                        <SubscribeButtonMenu
                            setIsFetchingSubscriptionSummary={setIsFetchingSubscriptionSummary}
                            setIsSubscribed={setIsSubscribed}
                            entityUrn={entityUrn}
                            entityData={entityData}
                            entityType={entityType}
                        />
                    </Menu>
                }
            >
                {renderSubscribeIcon()}
            </Dropdown>
        </ActionMenuItem>
    );
};
