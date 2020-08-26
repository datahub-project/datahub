import animate from '@f/animate';
import { begin as beginLoop, end as endLoop } from '@ember/runloop';

/**
 * Animate wrapper that is ember runloop friendly and promise friendly
 * @param start
 * @param end
 * @param fn
 */
function animateWrapper<T>(start: T, end: T, fn: (props: T, step: number) => void): Promise<void> {
  // Because animate actually uses RAF, run is not sufficient to make ember aware of this.
  beginLoop();
  return new Promise((success): void => {
    animate(start, end, (props: T, step: number): void => {
      fn(props, step);

      if (props === end) {
        success();
        endLoop();
      }
    });
  });
}

/**
 * Will transition and zoom into a position in the svg.
 *
 * This fn was extracted from https://github.com/APIs-guru/graphql-voyager/blob/b96ef1387c5f64867db338c1cfc3b070ebdd648a/src/graph/viewport.ts
 *
 * @param x
 * @param y
 * @param zoomEnd
 * @param zoomer
 */
export async function animatePanAndZoom(
  x: number,
  y: number,
  zoomEnd: number,
  zoomer: SvgPanZoom.Instance
): Promise<void> {
  const pan = zoomer.getPan();
  const panEnd = { x, y };
  await animateWrapper(pan, panEnd, (props): void => {
    zoomer.pan({ x: props.x, y: props.y });
  });

  const zoom = zoomer.getZoom();
  await animateWrapper({ zoom }, { zoom: zoomEnd }, (props): void => {
    zoomer.zoom(props.zoom);
  });
}

/**
 * Internal interface for focusElement for describing a DOMRect with different approach
 */
interface ICoords {
  // X start of rect
  x1: number;
  // X + width, end of rect
  x2: number;
  // y start of rect
  y1: number;
  // y + height, end of rect
  y2: number;
}

/**
 * Will transition and focus on multiple elements inside a svg
 *
 * extracted from https://github.com/APIs-guru/graphql-voyager/blob/b96ef1387c5f64867db338c1cfc3b070ebdd648a/src/graph/viewport.ts
 * @param el
 * @param svg
 * @param zoomer
 * @param maxZoom
 */
export async function focusElements(
  elements: Array<Element>,
  svg: Element,
  zoomer: SvgPanZoom.Instance,
  maxZoom: number,
  animate = true
): Promise<void> {
  const bbRect = svg.getBoundingClientRect();
  const offsetLeft = bbRect.left;
  const offsetTop = bbRect.top;
  const bbBoxes = elements.map(el => el.getBoundingClientRect());
  const currentPan = zoomer.getPan();
  const viewPortSizes = zoomer.getSizes();
  const toCoord = (bbBox: DOMRect): ICoords => ({
    x1: bbBox.x,
    x2: bbBox.x + bbBox.width,
    y1: bbBox.y,
    y2: bbBox.y + bbBox.height
  });
  const size = bbBoxes.reduce((sizes: ICoords | undefined, bbBox: DOMRect): ICoords => {
    const coords = toCoord(bbBox);
    return sizes
      ? {
          x1: Math.min(sizes.x1, coords.x1),
          x2: Math.max(sizes.x2, coords.x2),
          y1: Math.min(sizes.y1, coords.y1),
          y2: Math.max(sizes.y2, coords.y2)
        }
      : coords;
  }, undefined) || { x1: 0, x2: 0, y1: 0, y2: 0 };
  const width = size?.x2 - size?.x1;
  const height = size?.y2 - size?.y1;

  currentPan.x += viewPortSizes.width / 2 - width / 2;
  currentPan.y += viewPortSizes.height / 2 - height / 2;

  const zoomUpdateToFit = 1.2 * Math.max(height / viewPortSizes.height, width / viewPortSizes.width);
  let newZoom = zoomer.getZoom() / zoomUpdateToFit;
  const recomendedZoom = maxZoom * 0.6;
  if (newZoom > recomendedZoom) newZoom = recomendedZoom;

  const newX = currentPan.x - size.x1 + offsetLeft;
  const newY = currentPan.y - size.y1 + offsetTop;
  if (animate) {
    await animatePanAndZoom(newX, newY, newZoom, zoomer);
  } else {
    zoomer.pan({ x: newX, y: newY });
    zoomer.zoom(newZoom);
  }
}

/**
 * Will calculate the max zoom for a given svg
 *
 * Inspired in maxZoom fn from https://github.com/APIs-guru/graphql-voyager/blob/b96ef1387c5f64867db338c1cfc3b070ebdd648a/src/graph/viewport.ts
 * @param svgEl
 */
export function calculateMaxZoom(svgEl: SVGSVGElement): number {
  const svgWidth = svgEl.width.baseVal.value;
  const svgHeight = svgEl.height.baseVal.value;
  const constantWidth = 1800;
  const constantHeight = 1100;

  return Math.max(svgWidth / constantWidth, svgHeight / constantHeight);
}
