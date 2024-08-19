import React from 'react';
import { Image, Typography } from 'antd';
import { OnboardingStep } from '../OnboardingStep';
import { ANTD_GRAY } from '../../entity/shared/constants';
import dataHubFlowDiagram from '../../../images/datahub-flow-diagram-light.png';

export const GLOBAL_WELCOME_TO_DATAHUB_ID = 'global-welcome-to-datahub';
export const HOME_PAGE_INGESTION_ID = 'home-page-ingestion';
export const HOME_PAGE_DOMAINS_ID = 'home-page-domains';
export const HOME_PAGE_PLATFORMS_ID = 'home-page-platforms';
export const HOME_PAGE_MOST_POPULAR_ID = 'home-page-most-popular';
export const HOME_PAGE_SEARCH_BAR_ID = 'home-page-search-bar';

export const HomePageOnboardingConfig: OnboardingStep[] = [
    {
        id: GLOBAL_WELCOME_TO_DATAHUB_ID,
        content: (
            <div>
                <Image
                    preview={false}
                    height={184}
                    width={500}
                    style={{ marginLeft: '50px' }}
                    src={dataHubFlowDiagram}
                />
                <Typography.Title level={3}>Bem-vindo ao DataHub! 👋</Typography.Title>
                <Typography.Paragraph style={{ lineHeight: '22px' }}>
                    <strong>DataHub</strong> ajuda você a descobrir e organizar os dados importantes dentro do seu
                    organização. Você pode:
                </Typography.Paragraph>
                <Typography.Paragraph style={{ lineHeight: '24px' }}>
                    <ul>
                        <li>
                        Rapidamente <strong>busque</strong> para conjuntos de dados, painéis, pipelines de dados e muito mais
                        </li>
                        <li>
                        Visualize e entenda a <strong>linhagem ponta a ponta</strong> completa de como os dados são criados,
                        transformado e consumido
                        </li>
                        <li>
                        Obtenha <strong>insights</strong> sobre como outras pessoas da sua organização estão usando os dados
                        </li>
                        <li>
                            Defina <strong>propriedade</strong> e capture <strong>conhecimento</strong> para capacitar outras pessoas
                        </li>
                    </ul>
                    <p>Let&apos;s get started! 🚀</p>
                    <div
                        style={{
                            backgroundColor: ANTD_GRAY[4],
                            opacity: '0.7',
                            borderRadius: '4px',
                            height: '40px',
                            display: 'flex',
                            alignItems: 'center',
                        }}
                    >
                        <span style={{ paddingLeft: '5px' }}>💡</span>
                        <span style={{ paddingLeft: '10px' }}>
                        Pressione <strong> Cmd + Ctrl + T</strong> para abrir este tutorial a qualquer momento.                        </span>
                    </div>
                </Typography.Paragraph>
            </div>
        ),
        style: { minWidth: '650px' },
    },
    {
        id: HOME_PAGE_INGESTION_ID,
        selector: `#${HOME_PAGE_INGESTION_ID}`,
        title: 'Ingest Data',
        content: (
            <Typography.Paragraph>
Comece a integrar suas fontes de dados imediatamente navegando até a página <strong>Ingestão</strong>.            </Typography.Paragraph>
        ),
    },
    {
        id: HOME_PAGE_DOMAINS_ID,
        selector: `#${HOME_PAGE_DOMAINS_ID}`,
        title: 'Explore by Domain',
        content: (
            <Typography.Paragraph>
                Aqui estão os <strong>domínios</strong> da sua organização. Domínios são coleções de ativos de dados -
                como tabelas, painéis e modelos de ML - que facilitam a descoberta de informações relevantes para um
                parte específica da sua organização.
            </Typography.Paragraph>
        ),
    },
    {
        id: HOME_PAGE_PLATFORMS_ID,
        selector: `#${HOME_PAGE_PLATFORMS_ID}`,
        title: 'Explore by Platform',
        content: (
            <Typography.Paragraph>
                Aqui estão as <strong>plataformas de dados</strong> da sua organização. Plataformas de dados representam
                sistemas de dados ou ferramentas de terceiros. Os exemplos incluem data warehouses como <strong>Snowflake</strong>,
                Orquestradores como o <strong>Airflow</strong> e ferramentas de dashboard como o <strong>Looker</strong>.
            </Typography.Paragraph>
        ),
    },
    {
        id: HOME_PAGE_MOST_POPULAR_ID,
        selector: `#${HOME_PAGE_MOST_POPULAR_ID}`,
        title: 'Explorar os mais populares',
        content: "Aqui você encontrará os ativos que são visualizados com mais frequência na sua organização.",
    },
    {
        id: HOME_PAGE_SEARCH_BAR_ID,
        selector: `#${HOME_PAGE_SEARCH_BAR_ID}`,
        title: 'Find your Data 🔍',
        content: (
            <Typography.Paragraph>
                <p>
                Esta é a <strong>barra de pesquisa</strong>. Servirá como ponto de partida para descobrir e
                    colaborando em torno dos dados mais importantes para você.
                </p>
                <p>
                    Não sabe por onde começar? Clique em <strong>Explorar tudo</strong>!
                </p>
            </Typography.Paragraph>
        ),
    },
];
