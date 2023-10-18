docker build -t emqx emqx/
docker build -t data-injector data-injector/
docker build -t data-preprocessor data-preprocessor/
docker-compose up
