
dev:
    poetry lock
    poetry install --no-root


build-base:
    docker build -f common/Dockerfile.base -t $(basename $(pwd))/python-base:latest .

compose: build-base
    docker compose up --build --remove-orphans -d
    just compose_logs


down:
    docker compose down

compose_logs:
    docker compose logs -f

reload: down compose

running:
    docker ps

clean-docker:
    docker system prune -a -f --volumes

code2prompt:
    code2prompt ./ -e lock,env,drawio --exclude-files env.yml,.gitignore --exclude-folders tests
