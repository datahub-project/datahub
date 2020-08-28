import Component from '@glimmer/component';
import { action } from '@ember/object';
import { ETaskPromise } from '@datahub/utils/types/concurrency';
import { task } from 'ember-concurrency';
import { tracked } from '@glimmer/tracking';

const randomTag = ['funny', 'fail', 'cute', 'cat', 'puppy'];

export default class FoxieScreensRandomGif extends Component<{}> {
  baseUrl = 'https://api.giphy.com/v1/gifs/random';

  wrapperElement?: HTMLElement;

  get apiParams(): Record<string, string> {
    return {
      // eslint-disable-next-line @typescript-eslint/camelcase
      api_key: '0UTRbFtkMxAplrohufYco5IY74U8hOes',
      tag: randomTag[Math.floor(Math.random() * randomTag.length)],
      type: 'random',
      rating: 'pg-13'
    };
  }

  @tracked
  gifRenderData?: Record<string, unknown>;

  /**
   * Task defined to get the data necessary for this component to operate.
   */
  @task(function*(this: FoxieScreensRandomGif): IterableIterator<Promise<Response>> {
    const { apiParams, baseUrl } = this;
    const queryString = encodeURI(
      Object.keys(apiParams)
        .map(key => key + '=' + apiParams[key])
        .join('&')
    );

    const giphyApiUrl = `${baseUrl}?${queryString}`;
    const renderData = yield fetch(giphyApiUrl).then(response => response.json());
    this.gifRenderData = renderData;
  })
  getContainerDataTask!: ETaskPromise<Array<void>>;

  @action
  onDidInsert(element: HTMLElement): void {
    this.wrapperElement = element;
    this.getContainerDataTask.perform();
  }
}
