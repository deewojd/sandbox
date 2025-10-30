from abc import ABC, abstractmethod

# Interface Layer

class Job(ABC):
    """Abstract interface representing a data processing job."""
    
    @abstractmethod
    def update(self, filename: str) -> None:
        """Defines how a job handles a new data file."""
        pass

class Router(ABC):
    """Abstract interface for routing files to appropriate jobs."""
    
    @abstractmethod
    def register_job(self, keyword: str, job_instance: Job) -> None:
        pass

    @abstractmethod
    def route(self, filename: str) -> Job:
        pass

# Concrete Jobs

class ETLJob(Job):
    def update(self, filename: str) -> None:
        print(f"ETLJob: Processing {filename}")

class StreamingJob(Job):
    def update(self, filename: str) -> None:
        print(f"StreamingJob: Streaming {filename}")

class AnalyticsJob(Job):
    def update(self, filename: str) -> None:
        print(f"AnalyticsJob: Analyzing {filename}")

class ValidationJob(Job):
    def update(self, filename: str) -> None:
        print(f"ValidationJob: Validating {filename}")

# Router

class JobRouter(Router):
    """Maps filenames to registered jobs dynamically."""
    
    def __init__(self) -> None:
        self.jobs: dict[str, Job] = {}

    def register_job(self, keyword: str, job_instance: Job) -> None:
        self.jobs[keyword.lower()] = job_instance

    def route(self, filename: str) -> Job:
        for key, job in self.jobs.items():
            if key in filename.lower():
                return job
        return None

# Data Producer

class DataProducer:
    """Produces data events and automatically decides batch vs streaming."""
    
    def __init__(self, router: Router):
        self.router = router

    def produce(self, filenames):
        """
        Process files: streaming if 'stream' in filename, batch otherwise.
        """
        for filename in filenames:
            if "stream" in filename.lower():
                print(f"\nDataProducer (streaming): New file -> {filename}")
                yield filename  # streaming: caller can handle incrementally
            else:
                self.new_data_arrived(filename)

    def new_data_arrived(self, filename: str) -> None:
        """Batch mode: immediately notify router and update job."""
        print(f"\nDataProducer (batch): New file -> {filename}")
        job = self.router.route(filename)
        if job:
            job.update(filename)
        else:
            print(f"No job matched for {filename}")

# Example Usage

if __name__ == "__main__":
    router = JobRouter()

    # Register jobs
    router.register_job("etl", ETLJob())
    router.register_job("streaming", StreamingJob())
    router.register_job("analytics", AnalyticsJob())
    router.register_job("validation", ValidationJob())

    producer = DataProducer(router)

    files = [
        "sales_etl_2025.csv",
        "clickstream_streaming_2025.csv",
        "monthly_analytics_2025.csv",
        "user_validation_2025.csv",
        "unknown_file_2025.csv"
    ]

    # Produce batch and streaming files
    for file in producer.produce(files):
        # Only streaming files (yielded from produce) reach this point
        job = router.route(file)
        if job:
            job.update(file)
        else:
            print(f"No job matched for {file}")
