import { Badge, StructuredPopover, Text } from '@components';
import React from 'react';
import styled from 'styled-components';

import { AvatarStack } from '@components/components/AvatarStack/AvatarStack';
import HoverSectionContent from '@components/components/AvatarStack/HoverSectionContent';
import { AvatarStackProps, AvatarType } from '@components/components/AvatarStack/types';

import EntityRegistry from '@app/entityV2/EntityRegistry';
import StopPropagationWrapper from '@app/sharedV2/StopPropagationWrapper';

const HeaderContainer = styled.div`
    display: flex;
    gap: 4px;
`;

interface Props extends AvatarStackProps {
    entityRegistry: EntityRegistry;
}

const AvatarStackWithHover = ({
    avatars,
    size = 'default',
    showRemainingNumber = true,
    maxToShow = 4,
    entityRegistry,
    title = 'Owners',
}: Props) => {
    const users = avatars?.filter((avatar) => avatar.type === AvatarType.user) || [];
    const groups = avatars?.filter((avatar) => avatar.type === AvatarType.group) || [];
    const roles = avatars?.filter((avatar) => avatar.type === AvatarType.role) || [];

    const renderTitle = (headerText, count) => (
        <HeaderContainer>
            <Text size="sm" color="gray" weight="bold">
                {headerText}
            </Text>
            <Badge count={count} size="xs" />
        </HeaderContainer>
    );

    return (
        <StopPropagationWrapper>
            <StructuredPopover
                width={280}
                title={title}
                sections={[
                    ...(users.length > 0
                        ? [
                              {
                                  title: renderTitle('Users', users.length),
                                  content: (
                                      <HoverSectionContent
                                          avatars={users}
                                          entityRegistry={entityRegistry}
                                          size={size}
                                      />
                                  ),
                              },
                          ]
                        : []),
                    ...(groups.length > 0
                        ? [
                              {
                                  title: renderTitle('Groups', groups.length),
                                  content: (
                                      <HoverSectionContent
                                          avatars={groups}
                                          entityRegistry={entityRegistry}
                                          size={size}
                                          type={AvatarType.group}
                                      />
                                  ),
                              },
                          ]
                        : []),
                    ...(roles.length > 0
                        ? [
                              {
                                  title: renderTitle('Roles', roles.length),
                                  content: (
                                      <HoverSectionContent
                                          avatars={roles}
                                          entityRegistry={entityRegistry}
                                          size={size}
                                          type={AvatarType.role}
                                      />
                                  ),
                              },
                          ]
                        : []),
                ]}
            >
                <div>
                    <AvatarStack avatars={avatars} showRemainingNumber={showRemainingNumber} maxToShow={maxToShow} />
                </div>
            </StructuredPopover>
        </StopPropagationWrapper>
    );
};

export default AvatarStackWithHover;
