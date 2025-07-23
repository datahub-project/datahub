import { Icon, colors } from '@components';
import { Dropdown } from 'antd';
import React, { useCallback } from 'react';
import styled from 'styled-components';

import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import { DEFAULT_GLOBAL_MODULE_TYPES } from '@app/homeV3/modules/constants';
import { ModulePositionInput } from '@app/homeV3/template/types';

import { PageModuleFragment } from '@graphql/template.generated';

const StyledIcon = styled(Icon)`
    :hover {
        cursor: pointer;
    }
` as typeof Icon;

const DropdownWrapper = styled.div``;

interface Props {
    module: PageModuleFragment;
    position: ModulePositionInput;
}

export default function ModuleMenu({ module, position }: Props) {
    const { type } = module.properties;
    const canEdit = !DEFAULT_GLOBAL_MODULE_TYPES.includes(type);

    const {
        removeModule,
        moduleModalState: { openToEdit },
    } = usePageTemplateContext();

    const handleEditModule = useCallback(() => {
        openToEdit(type, module, position);
    }, [module, openToEdit, type, position]);

    const handleRemove = useCallback(() => {
        removeModule({
            module,
            position,
        });
    }, [removeModule, module, position]);

    const handleMenuClick = useCallback((e: React.MouseEvent) => {
        e.stopPropagation();
    }, []);

    return (
        <DropdownWrapper onClick={handleMenuClick}>
            <Dropdown
                trigger={['click']}
                menu={{
                    items: [
                        ...(canEdit
                            ? [
                                  {
                                      title: 'Edit',
                                      key: 'edit',
                                      label: 'Edit',
                                      style: {
                                          color: colors.gray[600],
                                          fontSize: '14px',
                                      },
                                      onClick: handleEditModule,
                                  },
                              ]
                            : []),
                        {
                            title: 'Remove',
                            label: 'Remove',
                            key: 'remove',
                            style: {
                                color: colors.red[500],
                                fontSize: '14px',
                            },
                            onClick: handleRemove,
                        },
                    ],
                }}
            >
                <StyledIcon icon="DotsThreeVertical" source="phosphor" size="lg" />
            </Dropdown>
        </DropdownWrapper>
    );
}
