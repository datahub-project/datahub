import { HasFailingAssertionsRenderer } from './assertion/HasFailingAssertionsRenderer';
import { DeprecationRenderer } from './deprecation/DeprecationRenderer';
import { FilterRenderer } from './FilterRenderer';
import { HasActiveIncidentsRenderer } from './incident/HasActiveIncidentsRenderer';
import { HasSiblingsRenderer } from './siblings/HasSiblingsRenderer';

export const renderers: Array<FilterRenderer> = [
    new HasFailingAssertionsRenderer(),
    new HasActiveIncidentsRenderer(),
    new HasSiblingsRenderer(),
    new DeprecationRenderer(),
];
