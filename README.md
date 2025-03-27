# Freight Stops Visualization and Analysis Software
This initial setup of this app was taken from <a href="https://github.com/dittonjs/4610Spring25ClassExamples">Joseph Ditton's repo</a>. Thank you Professor Ditton!
This initial setup of this app was taken from <a href="https://github.com/dittonjs/4610Spring25ClassExamples">Joseph Ditton's repo</a>. Thank you Professor Ditton!
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)

**Features**
- Handle MB/min of incoming information
- Prediction of route/delivery times
- Visualization of routes along a map interface
- Visualization of statistics relating to individual trips

## Setting up dev enviroment
1. Download <a href="https://www.docker.com/products/docker-desktop/">Docker Desktop</a>
2. Run Docker Desktop as an administrator
2. Open a terminal in the root project directory (the folder with docker-compose.yml)
3. Run `docker compose up`
4. If you do not have VSCode installed, <a href="https://code.visualstudio.com/">install it here.</a>.
5. Open VSCode and install the Dev Containers extension
6. Reopen VSCode in the server container
    1. In VSCode, press `Ctrl + Shift + P`
    2. Type "Reopen in Container" and press Enter
    3. Select *Server Container* and press Enter
7. Open a terminal in the Server Container
8. Run these commands:
    *   `npm run prisma-generate`
    *   `npm run prisma-migrate`
9. Restart the Server Container
    1. In the Docker Desktop app, *click Containers*
    2. In the list, click *freight-data*
    3. In the list of containers, click freight-data-server
    4. Click the restart button on the top right of the window
10. Visit `localhost:3000` and verify that the app is running.

When making any changes to the app, do it through the dev container. Reopen the specific container (as detailed in Step 7) and then make changes to the code. Through the power of Docker, these changes will be reflected in the actual directory.

You can make changes outside of the container, but you will need to install the node modules for each container and/or create a venv for the python packages

## Setting up the database. 
Find the monthly-split data files here 
https://usu.box.com/s/0r4puni3cx0mar4181faqvaj7uaku5l3
and here 
https://usu.box.com/s/63ziurrmh5hrqutouw0x89ad0tpefxd8
Because the routes data is too big, I have only included a couple of months, but this should be fine for development

1. Copy your data files into db_worker/monthly_route_data and db_worker/monthly_stop_data
2. Ensure the docker containers are running. You may need to restart the freight_db_worker container because I haven't figured out how to make it wait for rabbitMQ to finish starting before trying to connect
3. Run the commands 
    * `docker exec freight_db_worker python load_stop_data_into_db_parallel.py --month 1 --host db --password password` 
    * `docker exec freight_db_worker python load_route_data_into_db_parallel.py --month 1 --host db --password password`
    * **\*Note\*** these python scripts will use a lot of CPU power. Use the --workers option to specify how many processors should be used
4. Run the command `docker exec -it freight_db psql -U postgres -d mydatabase` and verify the tables were created using a command such as
```sql
SELECT *
FROM month_01_routes
WHERE ST_Within(
    location::geometry,
    ST_MakeEnvelope(-114.064453, 37.026061, -109.054687, 42.008507, 4326)
) LIMIT 10;
```