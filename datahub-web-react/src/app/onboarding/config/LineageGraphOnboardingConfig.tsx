import React from 'react';
import { Typography } from 'antd';
import { OnboardingStep } from '../OnboardingStep';

export const LINEAGE_GRAPH_INTRO_ID = 'lineage-graph-intro';
export const LINEAGE_GRAPH_TIME_FILTER_ID = 'lineage-graph-time-filter';

export const LineageGraphOnboardingConfig: OnboardingStep[] = [
    {
        id: LINEAGE_GRAPH_INTRO_ID,
        title: 'Lineage Graph',
        content: (
            <Typography.Paragraph>
                <p>
                Você pode visualizar o <strong>Gráfico de Linhagem</strong> de uma entidade nesta página.
                </p>
                <p>
                A <strong>Lineage</strong> de dados permite visualizar e compreender as dependências upstream
                    e consumidores a jusante desta entidade.
                </p>
                <p>
                    Saiba mais sobre <strong>Linhagem</strong>{' '}
                    <a
                        target="_blank"
                        rel="noreferrer noopener"
                        href="https://datahubproject.io/docs/generated/lineage/lineage-feature-guide/"
                    >
                        here.
                    </a>
                </p>
            </Typography.Paragraph>
        ),
    },
    {
        id: LINEAGE_GRAPH_TIME_FILTER_ID,
        selector: `#${LINEAGE_GRAPH_TIME_FILTER_ID}`,
        title: 'Filter Lineage Edges by Date',
        content: (
            <Typography.Paragraph>
                <p>
                Você pode clicar em quais datas deseja ver as bordas da linhagem neste gráfico. Por padrão, o
                    O gráfico mostrará as bordas observadas nos últimos 14 dias. Observe que as arestas de linhagem manual e as arestas sem
                    as informações de horário sempre serão mostradas.
                </p>
            </Typography.Paragraph>
        ),
    },
];
