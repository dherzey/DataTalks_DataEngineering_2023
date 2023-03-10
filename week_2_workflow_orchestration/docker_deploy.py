from prefect.infrastructure.docker import DockerContainer
from prefect.deployments import Deployment
from parameterized_flow import etl_parent_flow
from prefect.orion.schemas.schedules import CronSchedule

docker_block = DockerContainer.load("zoomcamp-docker-container")
docker_dep = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name="docker-flow",
    infrastructure=docker_block #run our flow locally in Docker
    # schedule=(CronSchedule(cron="0 5 1 * *", timezone="UTC"))
)

if __name__=="__main__":
    docker_dep.apply()