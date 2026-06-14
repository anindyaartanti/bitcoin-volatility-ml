from prefect.deployments import Deployment
from prefect.client.schemas.schedules import IntervalSchedule
from prefect.client.schemas.objects import MinimalDeploymentSchedule

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from sentiment_flow import sentiment_pipeline
from training_flow import deploy as deploy_training


if __name__ == "__main__":
    Deployment.build_from_flow(
        flow=sentiment_pipeline,
        name="sentiment-pipeline",
        work_pool_name="default",
        schedules=[MinimalDeploymentSchedule(
            schedule=IntervalSchedule(interval=1800)  # 30 menit
        )],
        apply=True,
    )
    print("Deployment 'sentiment-pipeline' created.")

    deploy_training()
