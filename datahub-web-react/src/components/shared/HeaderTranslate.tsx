/* eslint-disable jsx-a11y/click-events-have-key-events */
/* eslint-disable jsx-a11y/no-noninteractive-element-interactions */
import React from 'react';
import { TranslationOutlined } from '@ant-design/icons';
import { Dropdown, Button, message } from 'antd';
import type { MenuProps } from 'antd';
import { useTranslation } from 'react-i18next'

// TODO Move this to user settings - language option
export const HeaderTranslate: React.FC = () => {
    const { i18n } = useTranslation()
    const items: MenuProps['items'] = [
        {
            key: '1',
            label: (
                <p  onClick={() => { i18n.changeLanguage('zh')
                    .then(message.success("中文设置成功") ); }}>简体中文</p>
            ),
        },
        {
            key: '2',
            label: (
                <p   onClick={() => { i18n.changeLanguage('en')
                    .then(message.success("Done") ); }}>English</p>
            ),
        },
    ]
    return (
        <span>
      <Dropdown menu={{ items }}>
        <Button type="text" aria-hidden="true" onClick={(e) => e.preventDefault()}>
          <TranslationOutlined />
        </Button>
      </Dropdown>
    </span>
    );
};
