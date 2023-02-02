from prefect_github.repository import GitHubRepository

github_block = GitHubRepository(
    repository_url="https://github.com/dherzey/DataTalks_DataEngineering_2023.git"
)

github_block.save("zoomcamp-github", overwrite=True)