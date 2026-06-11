from prefect.deployments import Deployment
from prefect.client.schemas.schedules import CronSchedule, IntervalSchedule
from prefect.client.schemas.objects import MinimalDeploymentSchedule

from model_training import model_training_flow
from sentiment_flow import sentiment_pipeline
from sentiment_processing import sentiment_processing_flow
from twitter_ingestion import twitter_ingestion_flow


if __name__ == "__main__":
    Deployment.build_from_flow(
        flow=twitter_ingestion_flow,
        name="twitter-ingestion",
        work_pool_name="default",
        schedules=[MinimalDeploymentSchedule(schedule=CronSchedule(cron="0 * * * *"))],
        apply=True,
    )
    print("Deployment 'twitter-ingestion' created.")

    Deployment.build_from_flow(
        flow=sentiment_processing_flow,
        name="sentiment-processing",
        work_pool_name="default",
        apply=True,
    )
    print("Deployment 'sentiment-processing' created.")

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

    Deployment.build_from_flow(
        flow=model_training_flow,
        name="model-training",
        work_pool_name="default",
        schedules=[MinimalDeploymentSchedule(schedule=CronSchedule(cron="0 2 * * 1"))],
        apply=True,
    )
    print("Deployment 'model-training' created.")
