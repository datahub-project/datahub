import React from 'react';
import { Typography } from 'antd';
import { OnboardingStep } from '../OnboardingStep';

export const SEARCH_RESULTS_FILTERS_ID = 'search-results-filters';
export const SEARCH_RESULTS_ADVANCED_SEARCH_ID = 'search-results-advanced-search';
export const SEARCH_RESULTS_BROWSE_SIDEBAR_ID = 'search-results-browse-sidebar';
export const SEARCH_RESULTS_FILTERS_V2_INTRO = 'search-results-filters-v2-intro';

export const SearchOnboardingConfig: OnboardingStep[] = [
    {
        id: SEARCH_RESULTS_FILTERS_ID,
        selector: `#${SEARCH_RESULTS_FILTERS_ID}`,
        title: '🕵️ Narrow your search',
        content: (
            <Typography.Paragraph>
                Encontre rapidamente ativos relevantes aplicando um ou mais filtros. Tente filtrar por <strong>Tipo</strong>,{' '}
                <strong>Proprietário</strong> e muito mais!
            </Typography.Paragraph>
        ),
    },
    {
        id: SEARCH_RESULTS_ADVANCED_SEARCH_ID,
        selector: `#${SEARCH_RESULTS_ADVANCED_SEARCH_ID}`,
        title: '💪 Dive deeper with advanced filters',
        content: (
            <Typography.Paragraph>
                <strong>Filtros Avançados</strong> oferecem recursos adicionais para criar consultas de pesquisa mais específicas.            </Typography.Paragraph>
        ),
    },
    {
        id: SEARCH_RESULTS_BROWSE_SIDEBAR_ID,
        selector: `#${SEARCH_RESULTS_BROWSE_SIDEBAR_ID}`,
        title: '🧭 Explore and refine your search by platform',
        style: { minWidth: '425px' },
        content: (
            <Typography.Paragraph>
                Tem uma ideia clara do esquema ou pasta que está procurando? Navegue facilmente pelo seu
                plataformas da organização inline. Em seguida, selecione um contêiner específico onde deseja filtrar seus resultados
                por.
            </Typography.Paragraph>
        ),
    },
    {
        id: SEARCH_RESULTS_FILTERS_V2_INTRO,
        prerequisiteStepId: SEARCH_RESULTS_FILTERS_ID,
        selector: `#${SEARCH_RESULTS_FILTERS_V2_INTRO}`,
        title: 'Filters Have Moved',
        content: (
            <Typography.Paragraph>
              Encontre rapidamente ativos relevantes com nossa interface de filtro nova e aprimorada! Nossa última atualização foi realocada
              filtros na parte superior da tela para facilitar o acesso.
            </Typography.Paragraph>
        ),
    },
];
