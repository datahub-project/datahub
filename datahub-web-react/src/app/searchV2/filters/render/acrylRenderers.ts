import { FilterRenderer } from '@app/searchV2/filters/render/FilterRenderer';
import { HasFailingAssertionsRenderer } from '@app/searchV2/filters/render/assertion/HasFailingAssertionsRenderer';
import { DeprecationRenderer } from '@app/searchV2/filters/render/deprecation/DeprecationRenderer';
import { HasActiveIncidentsRenderer } from '@app/searchV2/filters/render/incident/HasActiveIncidentsRenderer';
import { HasSiblingsRenderer } from '@app/searchV2/filters/render/siblings/HasSiblingsRenderer';
import { HasFailingTestsRenderer } from '@app/searchV2/filters/render/test/HasFailingTestsRenderer';

export const renderers: Array<FilterRenderer> = [
    new HasFailingTestsRenderer(),
    new HasFailingAssertionsRenderer(),
    new HasActiveIncidentsRenderer(),
    new HasSiblingsRenderer(),
    new DeprecationRenderer(),
];
