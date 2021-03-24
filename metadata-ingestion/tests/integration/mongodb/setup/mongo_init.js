
db.mycollection.createIndex({ myfield: 1 }, { unique: true }),
db.mycollection.createIndex({ thatfield: 1 }),
db.mycollection.insert({ myfield: 'hello1', thatfield: 'testing', noindex: 8}),
db.mycollection.insert({ myfield: 'hello2', thatfield: 'testing', noindex: 2}),
db.mycollection.insert({ myfield: 'hello3', thatfield: 'testing', noindex: 5}),
db.mycollection.insert({ myfield: 'hello5', thatfield: 'testing', noindex: 2})
