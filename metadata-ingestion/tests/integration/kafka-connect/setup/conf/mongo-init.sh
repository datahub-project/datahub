#!/bin/bash

mongo -- "$MONGO_INITDB_DATABASE" <<-EOJS
    db.purchases.insert({ _id: 18576345, item: "lamp post", price: 34.7 });
EOJS

{
sleep 3 &&
mongo -- "$MONGO_INITDB_DATABASE" <<-EOJS
    var rootUser = '$MONGO_INITDB_ROOT_USERNAME';
    var rootPassword = '$MONGO_INITDB_ROOT_PASSWORD';
    var admin = db.getSiblingDB('admin');
    admin.auth(rootUser, rootPassword);
    rs.initiate();
EOJS
} &