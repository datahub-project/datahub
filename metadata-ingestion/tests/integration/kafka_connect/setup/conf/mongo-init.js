rsconf = {
  _id: "rs0",
  members: [{ _id: 0, host: "test_mongo:27017", priority: 1.0 }],
};
rs.initiate(rsconf);
rs.status();
