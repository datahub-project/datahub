import React from 'react';
import { Typography } from 'antd';
import { OnboardingStep } from '../OnboardingStep';

export const BUSINESS_GLOSSARY_INTRO_ID = 'business-glossary-intro';
export const BUSINESS_GLOSSARY_CREATE_TERM_ID = 'business-glossary-create-term';
export const BUSINESS_GLOSSARY_CREATE_TERM_GROUP_ID = 'business-glossary-create-term-group';

export const BusinessGlossaryOnboardingConfig: OnboardingStep[] = [
    {
        id: BUSINESS_GLOSSARY_INTRO_ID,
        title: '数据字典 📖',
        content: (
            <Typography.Paragraph>
                <p>
                    欢迎使用 <strong>数据字典</strong>!
                </p>
                <p>
                    数据字典采用结构化，标准化的名称来分类管理您的数据资产.
                    您可以通过创建与使用 <strong>术语</strong> 和 <strong>术语组</strong> 进行管理.
                </p>
            </Typography.Paragraph>
        ),
    },
    {
        id: BUSINESS_GLOSSARY_CREATE_TERM_ID,
        selector: `#${BUSINESS_GLOSSARY_CREATE_TERM_ID}`,
        title: '术语',
        content: (
            <Typography.Paragraph>
                <p>
                    创建 <strong>术语</strong> .
                </p>
                <p>
                    <strong>术语</strong> 是特定业务含义的专有名词或句子。
                </p>
            </Typography.Paragraph>
        ),
    },
    {
        id: BUSINESS_GLOSSARY_CREATE_TERM_GROUP_ID,
        selector: `#${BUSINESS_GLOSSARY_CREATE_TERM_GROUP_ID}`,
        title: '术语组',
        content: (
            <Typography.Paragraph>
                <p>
                    创建 <strong>术语组</strong>.
                </p>
                <p>
                    <strong>术语组</strong> 采用文件夹结构来管理术语及其它术语组。
                </p>
                <p>
                    例如，术语组<strong>PII Term Group</strong> 可能包含不同类型的PII术语,
                    比如 <strong>Email</strong> 或者 <strong>Phone Number</strong>.
                </p>
                <p>
                    学习更多 <strong>术语</strong>{' '}
                    <a
                        target="_blank"
                        rel="noreferrer noopener"
                        href="https://datahubproject.io/docs/glossary/business-glossary"
                    >
                        {' '}
                        here.
                    </a>
                </p>
            </Typography.Paragraph>
        ),
    },
];
