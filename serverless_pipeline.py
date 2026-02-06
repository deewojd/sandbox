# Serverless Example: Event-Driven Data Pipeline (Decoupled Design)
"""
An Event-Driven Data Pipeline is the distributed evolution of the Observer pattern. 
It takes the behavioral concept of "tell me when something happens" and scales it up to work across entire cloud ecosystems.

Event → Router → Stateless Function → Output

Instead of one monolith doing everything, each function handles one responsibility. 
"""

# Transform
def transform_data(event):
    """Simulates a serverless transformation function."""
    data = event["payload"]
    result = [x.upper() for x in data]
    return {"status": "processed", "data": result}

# Load
def load_to_warehouse(event):
    """Simulates loading data into a warehouse."""
    print(f"Loading into warehouse: {event['data']}")
    return {"status": "loaded"}


# Event router (simulates cloud event triggers)
def event_router(event):
    # Event triggered
    if event["type"] == "file_uploaded":
        transformed = transform_data(event)
        return load_to_warehouse(transformed)
    # Scheduled
    elif event["type"] == "scheduled_job":
        return transform_data(event)

    else:
        return {"status": "ignored"}


# Simulated trigger
if __name__ == "__main__":
    event = {
        "type": "file_uploaded",
        "payload": ["ny", "nj", "fl"]
    }

    # ETL: Event triggers Transform and Load steps
    response = event_router(event)
    print(response)
