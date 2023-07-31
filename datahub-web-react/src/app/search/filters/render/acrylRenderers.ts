import { HasFailingAssertionsRenderer } from './assertion/HasFailingAssertionsRenderer';
import { FilterRenderer } from './FilterRenderer';
import { HasActiveIncidentsRenderer } from './incident/HasActiveIncidentsRenderer';

export const renderers: Array<FilterRenderer> = [new HasFailingAssertionsRenderer(), new HasActiveIncidentsRenderer()];
