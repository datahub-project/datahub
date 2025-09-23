import { Tabs as AntTabs } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { Button } from '@components/components/Button';
import { colors } from '@components/theme';

const StyledSubTabs = styled(AntTabs)<{
    $navMarginBottom?: number;
    $navMarginTop?: number;
    $containerHeight?: 'full' | 'auto';
    $addPaddingLeft?: boolean;
    $scrollable?: boolean;
    $stickyHeader?: boolean;
}>`
    ${({ $scrollable, $containerHeight }) => {
        if (!$scrollable) {
            if ($containerHeight === 'full') {
                return 'height: 100%;';
            }
            return 'flex: 1;';
        }
        return '';
    }}
    ${({ $scrollable }) => !$scrollable && 'overflow: hidden;'}

    ${({ $addPaddingLeft }) =>
        $addPaddingLeft
            ? `
			.ant-tabs-tab {
				margin-left: 6px !important;
			}
			`
            : `
			.ant-tabs-tab + .ant-tabs-tab {
				margin-left: 6px !important;
			}
		`}
	${({ $stickyHeader }) =>
        $stickyHeader &&
        `
			.ant-tabs-nav {
				position: sticky;
				top: 0;
				z-index: 10;
				background-color: white;
			}
		`}

    .ant-tabs-ink-bar {
        display: none;
    }

    .ant-tabs-nav {
        margin-bottom: ${(props) => props.$navMarginBottom ?? 8}px;
        margin-top: ${(props) => props.$navMarginTop ?? 0}px;

        &::before {
            display: none;
        }
    }
`;

const StyledButton = styled(Button)<{ $selected?: boolean }>`
    ${({ $selected }) => !$selected && `color: ${colors.gray[600]};`}
    ${({ $selected }) => $selected && `pointer-events: none;`}
`;

export interface SubTab {
    name: string;
    key: string;
    component: JSX.Element;
    count?: number;
    onSelectTab?: () => void;
    dataTestId?: string;
    disabled?: boolean;
}

function SubTabView({ subTab, selected }: { subTab: SubTab; selected: boolean }) {
    return (
        <StyledButton $selected={selected} variant={selected ? 'secondary' : 'text'}>
            {subTab.name}
        </StyledButton>
    );
}

interface Props {
    subTabs?: SubTab[];
    styleOptions?: {
        containerHeight?: 'full' | 'auto';
        navMarginBottom?: number;
        navMarginTop?: number;
        addPaddingLeft?: boolean;
        scrollToTopOnChange?: boolean;
        stickyHeader?: boolean;
    };
}

export default function SubTabs({ subTabs, styleOptions }: Props) {
    const [selectedTabKey, setSelectedTabKey] = useState<string | undefined>(subTabs?.[0]?.key);

    if (!subTabs || !(subTabs.length > 0)) return null;

    // add useMemo here
    const items = subTabs.map((tab) => ({
        key: tab.key,
        label: <SubTabView subTab={tab} selected={selectedTabKey === tab.key} />,
        disabled: tab.disabled,
        children: tab.component,
    }));

    return (
        <StyledSubTabs
            activeKey={selectedTabKey}
            items={items}
            onChange={(key) => setSelectedTabKey(key)}
            $navMarginBottom={styleOptions?.navMarginBottom}
            $navMarginTop={styleOptions?.navMarginTop}
            $containerHeight={styleOptions?.containerHeight}
            $addPaddingLeft={styleOptions?.addPaddingLeft}
            $scrollable={styleOptions?.scrollToTopOnChange}
            $stickyHeader={styleOptions?.stickyHeader}
        />
    );
}
