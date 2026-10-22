# Software Architecture for Big Data - Exercise 4

This exercise focuses on containerization of software.
We are going to put the HTTP server from the last exercise
into a Docker container, using a multi-stage Dockerfile.

We are also going to add a PostgreSQL database to this project.
The database shall be started as a docker container.

## Todo
- [ ] Start a PostgreSQL docker container (`postgres:18`) using plain docker commands
    - Use environment variables in db
    - Map the PostgreSQL storage (`/var/lib/postgresql/18/docker`) to a Docker volume to make it persistent
    - Connect a database viewer to the db ([GoLand](https://www.jetbrains.com/help/go/quick-start-with-database-functionality.html), [VSCode](https://marketplace.visualstudio.com/items?itemName=ms-ossdata.vscode-pgsql), [DBeaver](https://dbeaver.io/))
- [ ] Prepopulate database in code
- [ ] Create multi-stage Dockerfile
    - First stage (`builder`) should compile the application
        - Use `golang:1.27` as base image for the `builder` stage
        - During the first stage copy all contents to `/app` directory and execute the `scripts/build-application.sh` script
        - The `script/build-application.sh` creates a binary located at `/app/ordersystem`
      - Second stage should copy the compiled binary from `builder` and start the application on container run
          - Use `alpine` as base image for the `run` stage
- [ ] Build & run Orderservice container
  - Use environment variables in oderservice
- [ ] Create scripts containing all commands you needed to build and run the orderservice as container and start the database.

Environment variables:

```env
POSTGRES_DB=order
POSTGRES_USER=docker
POSTGRES_PASSWORD=docker
PGPORT=5432
DB_HOST=127.0.0.1
```

## Tips and Tricks

Have a look at the official documentation
about [multi-stage Dockerfiles](https://docs.docker.com/build/building/multi-stage/).
The container image can be built using: `docker build -t orderservice .`.
Use the flag `--network host` for `docker run` to let both containers run directly on the host network.
Pass the whole environment file with: `--env-file debug.env`.

The Docker documentation will be very helpful: https://docs.docker.com/get-started/workshop/02_our_app/
We're using Gorm as ORM, you can find the documentation for this framework
[here](https://gorm.io/docs/index.html).

To manage local Docker containers in a convenient way,
you can use [lazydocker](https://github.com/jesseduffield/lazydocker).

Docker commands are documented here: [Docker](https://docs.docker.com/reference/cli/docker/) ([Cheatsheet](https://docs.docker.com/get-started/docker_cheatsheet.pdf))

