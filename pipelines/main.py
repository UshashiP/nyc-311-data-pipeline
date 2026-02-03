"""Entry point to run all pipeline scripts."""
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from nyc311_data import ingest_raw_data
from transform import transform_data
from build_analytics import build_star_schema
from run_queries import execute_queries


class NYC311Pipeline:
    """Orchestrates the NYC 311 data pipeline."""
    
    def __init__(self):
        self.steps = [
            ("Ingesting raw data", ingest_raw_data),
            ("Transforming and cleaning data", transform_data),
            ("Building analytics star schema", build_star_schema),
            ("Running analytical queries", execute_queries),
        ]
    
    def run(self):
        """Execute all pipeline steps."""
        print("Starting NYC 311 Data Pipeline")
        
        for i, (description, step_func) in enumerate(self.steps, 1):
            
            print(f"Step {i}/{len(self.steps)}: {description}")
            
            try:
                step_func()
            except Exception as e:
                print(f"Step {i} failed: {e}")
                raise
        
        print("Pipeline completed successfully!")
      

def main():
    pipeline = NYC311Pipeline()
    pipeline.run()


if __name__ == "__main__":
    main()