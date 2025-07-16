import { Icon, colors } from '@components';
import { Dropdown } from 'antd';
import React, { useCallback } from 'react';
import styled from 'styled-components';

import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import { DEFAULT_GLOBAL_MODULE_TYPES } from '@app/homeV3/modules/constants';

import { PageModuleFragment } from '@graphql/template.generated';

const StyledIcon = styled(Icon)`
    :hover {
        cursor: pointer;
    }
` as typeof Icon;

interface Props {
    module: PageModuleFragment;
}

export default function ModuleMenu({ module }: Props) {
    const { type } = module.properties;

    const {
        createModuleModalState: { openToEdit },
    } = usePageTemplateContext();

    const handleEditModule = useCallback(() => {
        openToEdit(type, module);
    }, [module, openToEdit, type]);

    const canEdit = !DEFAULT_GLOBAL_MODULE_TYPES.includes(type);

    return (
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
                        title: 'Delete',
                        label: 'Delete',
                        key: 'delete',
                        style: {
                            color: colors.red[500],
                            fontSize: '14px',
                        },
                    },
                ],
            }}
        >
            <StyledIcon icon="DotsThreeVertical" source="phosphor" size="lg" />
        </Dropdown>
    );
}
