from prefect.infrastructure.docker import DockerContainer
from prefect.deployments import Deployment
from parameterized_gcs_to_bq import etl_parent_flow

docker_block = DockerContainer.load("zoomcamp-docker-container")
docker_dep = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name="docker-bq-flow",
    infrastructure=docker_block
)

if __name__=="__main__":
    docker_dep.apply()