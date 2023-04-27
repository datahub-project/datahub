#!/bin/bash

mongo -- "$MONGO_INITDB_DATABASE" <<-EOJS
    conn = new Mongo();
    db = conn.getDB("test_db");
    db.purchases.insertOne({ _id: 3, item: "lamp post", price: 12 });
    db.purchases.insertOne({ _id: 4, item: "lamp post", price: 13 });
EOJS


{
sleep 3 &&
mongo -- "$MONGO_INITDB_DATABASE" <<-EOJS
    var rootUser = '$MONGO_INITDB_ROOT_USERNAME';
    var rootPassword = '$MONGO_INITDB_ROOT_PASSWORD';
    var admin = db.getSiblingDB('admin');
    admin.auth(rootUser, rootPassword);
EOJS
} &



