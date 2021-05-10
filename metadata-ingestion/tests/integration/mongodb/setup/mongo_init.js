db.firstCollection.createIndex({ myfield: 1 }, { unique: true });
db.firstCollection.createIndex({ thatfield: 1 });
db.firstCollection.insertMany([
  { myfield: "hello1", thatfield: "testing", noindex: 8 },
  { myfield: "hello2", thatfield: "testing", noindex: 2 },
  { myfield: "hello3", thatfield: "testing", noindex: 5 },
  { myfield: "hello5", thatfield: "testing", noindex: 2 },
]);
