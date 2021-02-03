CONTAINER_NAME="testsqlserver"
ready="SQL Server is now ready for client connections."
until docker logs $CONTAINER_NAME | grep "$ready";
do
   echo "sleeping for 5 seconds"
   sleep 5
done



