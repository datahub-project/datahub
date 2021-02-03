dir=`pwd`
CONTAINER_NAME="testsqlserver"
docker run -d --rm --name=${CONTAINER_NAME} -e 'ACCEPT_EULA=Y' -e 'SA_PASSWORD=test!Password' -p 1433:1433 -v $dir/setup:/setup mcr.microsoft.com/mssql/server:latest
ready="SQL Server is now ready for client connections."
until docker logs $CONTAINER_NAME | grep -q "$ready";
do
   echo "sleeping for 5 seconds"
   sleep 5
done
echo "SQL Server started"


