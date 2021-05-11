db.firstCollection.createIndex({ name: 1 }, { unique: true });
db.firstCollection.createIndex({ legs: 1 });
db.firstCollection.createIndex({ floatField: 1 });
db.firstCollection.createIndex({ floatField: 1 });
db.firstCollection.insertMany([
  {
    name: "garfield",
    type: "cat",
    legs: 4,
    age: 8.3,
    tags: ["lasagna", "mondays", "sleep", "lazy"],
    seen: Date.now(),
    canSwim: false,
    favoriteFood: {
      name: "lasagna",
      calories: 4000,
      ingredients: [
        { name: "pasta", from: "italy" },
        { name: "cheese", from: "california" },
        { name: "tomatoes", from: "whole foods" },
      ],
      servings: 43.2,
      emptyObject: {},
    },
    favoriteColor: "orange",
  },
  {
    name: "odie",
    type: "dog",
    legs: 4,
    age: 5.2,
    tags: ["woof"],
    seen: Date.now(),
    canSwim: false,
    favoriteFood: {
      name: "dog food",
      calories: 1000,
      ingredients: [{ name: "mystery meat", from: "walmart" }],
    },
    servings: 1,
    emptyObject: {},
  },
  {
    name: "jon",
    type: "human",
    legs: 2,
    age: 32.1,
    tags: ["boring"],
    seen: Date.now(),
    canSwim: false,
    favoriteFood: {
      name: "who knows",
      calories: null,
      ingredients: [],
    },
    servings: 1,
  },
  {
    name: "george",
    type: "octopus",
    legs: 8,
    age: 23,
    tags: ["blub", "glug", "ink"],
    seen: Date.now(),
    canSwim: true,
    favoriteFood: {
      name: "crab",
      calories: 500,
      ingredients: [{ name: "crab", from: "the bay" }],
    },
    servings: 1,
  },
]);

db.emptyCollection.createIndex({ stringField: 1 }, { unique: true });
