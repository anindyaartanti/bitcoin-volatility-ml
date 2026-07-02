from prefect.deployments import Deployment
from prefect.client.schemas.schedules import IntervalSchedule
from prefect.client.schemas.objects import MinimalDeploymentSchedule

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from sentiment_flow import sentiment_pipeline
from training_flow import deploy as deploy_training
from system_health_check import deploy as deploy_health_check
from data_quality_check import deploy as deploy_dq_check
from model_performance_check import deploy as deploy_model_perf
from log_ingester import deploy as deploy_log_ingester


if __name__ == "__main__":
    Deployment.build_from_flow(
        flow=sentiment_pipeline,
        name="sentiment-pipeline",
        work_pool_name="default",
        schedules=[MinimalDeploymentSchedule(
            schedule=IntervalSchedule(interval=1800)
        )],
        apply=True,
    )
    print("Deployment 'sentiment-pipeline' created.")

    deploy_training()

    deploy_health_check()

    deploy_dq_check()

    deploy_model_perf()

    deploy_log_ingester()
