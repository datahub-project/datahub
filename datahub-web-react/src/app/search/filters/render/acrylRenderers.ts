import { FilterRenderer } from '@app/search/filters/render/FilterRenderer';
import { HasFailingAssertionsRenderer } from '@app/search/filters/render/assertion/HasFailingAssertionsRenderer';
import { HasActiveIncidentsRenderer } from '@app/search/filters/render/incident/HasActiveIncidentsRenderer';

export const renderers: Array<FilterRenderer> = [new HasFailingAssertionsRenderer(), new HasActiveIncidentsRenderer()];
