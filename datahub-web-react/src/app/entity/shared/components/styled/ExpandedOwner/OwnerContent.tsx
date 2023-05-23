import { Popover, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { Owner } from '../../../../../../types.generated';
import { CustomAvatar } from '../../../../../shared/avatar';
import { getDescriptionFromType, getNameFromType } from '../../../containers/profile/sidebar/Ownership/ownershipUtils';

const TextWrapper = styled.span<{ fontSize?: number }>`
    ${(props) => props.fontSize && `font-size: ${props.fontSize}px;`}
`;

const ContentWrapper = styled.span`
    display: flex;
    align-items: center;
`;

interface Props {
    name: string;
    owner: Owner;
    hidePopOver?: boolean;
    pictureLink?: string;
    fontSize?: number;
}

export default function OwnerContent({ name, owner, hidePopOver, pictureLink, fontSize }: Props) {
    return (
        <ContentWrapper>
            <CustomAvatar name={name} photoUrl={pictureLink} useDefaultAvatar={false} />
            {hidePopOver ? (
                <TextWrapper fontSize={fontSize}>{name}</TextWrapper>
            ) : (
                <Popover
                    overlayStyle={{ maxWidth: 200 }}
                    placement="left"
                    title={<Typography.Text strong>{getNameFromType(owner.type)}</Typography.Text>}
                    content={<Typography.Text type="secondary">{getDescriptionFromType(owner.type)}</Typography.Text>}
                >
                    <TextWrapper fontSize={fontSize}>{name}</TextWrapper>
                </Popover>
            )}
        </ContentWrapper>
    );
}
