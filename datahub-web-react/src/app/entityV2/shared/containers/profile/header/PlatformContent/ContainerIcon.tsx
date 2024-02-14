import { FolderOpenOutlined } from '@ant-design/icons';
import { Maybe } from 'graphql/jsutils/Maybe';
import React from 'react';
import styled from 'styled-components';
import { Container } from '../../../../../../../types.generated';
import { getSubTypeIcon, TYPE_ICON_CLASS_NAME } from '../../../../components/subtypes';
import { SEARCH_COLORS } from '../../../../constants';

const IconWrapper = styled.span`
    line-height: 0;
    .${TYPE_ICON_CLASS_NAME} {
        font-size: 14px;
    }
    // img,
    // svg {
    //     height: 12px;
    // }
`;

const DefaultIcon = styled(FolderOpenOutlined)`
    color: ${SEARCH_COLORS.PLATFORM_TEXT};

    &&& {
        font-size: 16px;
    }
`;

interface Props {
    container: Maybe<Container>;
}

export default function ContainerIcon({ container }: Props): JSX.Element {
    return (
        <IconWrapper>
            <ContainerIconBase container={container} />
        </IconWrapper>
    );
}

export function ContainerIconBase({ container }: Props): JSX.Element {
    const subtype = container?.subTypes?.typeNames?.[0].toLowerCase();
    return (subtype && getSubTypeIcon(subtype)) || <DefaultIcon />;
}
