import { Popover, Typography } from 'antd';
import React from 'react';
import { Owner } from '../../../../../../types.generated';
import { CustomAvatar } from '../../../../../shared/avatar';
import { getDescriptionFromType, getNameFromType } from '../../../containers/profile/sidebar/Ownership/ownershipUtils';

interface Props {
    name: string;
    owner: Owner;
    hidePopOver?: boolean;
    pictureLink?: string;
}

export default function OwnerContent({ name, owner, hidePopOver, pictureLink }: Props) {
    return (
        <>
            <CustomAvatar name={name} photoUrl={pictureLink} useDefaultAvatar={false} />
            {(hidePopOver && <>{name}</>) || (
                <Popover
                    overlayStyle={{ maxWidth: 200 }}
                    placement="left"
                    title={<Typography.Text strong>{getNameFromType(owner.type)}</Typography.Text>}
                    content={<Typography.Text type="secondary">{getDescriptionFromType(owner.type)}</Typography.Text>}
                >
                    {name}
                </Popover>
            )}
        </>
    );
}
