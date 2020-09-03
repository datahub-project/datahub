import { Factory, faker, trait } from 'ember-cli-mirage';

const staticUrls = [
  'https://pmcvariety.files.wordpress.com/2019/05/pokemon-detective-pikachu.jpg',
  'https://nomoreworkhorse.files.wordpress.com/2019/05/pokemon-detective-pikachu-card-1167030-1280x0.jpeg'
];

const staticActors = ['aketchum', 'goak'];

export default Factory.extend({
  url: faker.internet.url,
  description() {
    return faker.random.words(7);
  },
  createStamp() {
    return {
      actor: faker.name.lastName(),
      time: new Date(faker.date.recent(20)).getTime()
    };
  },
  static: trait({
    url(id: number) {
      return staticUrls[id % staticUrls.length];
    },
    description: 'Link description',
    createStamp(id: number) {
      return {
        actor: staticActors[id % staticActors.length],
        time: new Date(faker.date.recent(20)).getTime()
      };
    }
  })
});
