import os
import pandas as pd

# Base directories
aux_dir = r"C:\Users\Mohamed Mohsin\python tests\MnHna\CS\data\aux_by_month"
tickets_dir = r"C:\Users\Mohamed Mohsin\python tests\MnHna\CS\data\tickets_by_month"

# Entities in aux_by_month
aux_entities = ["recordings", "ivr_paths", "call_events"]

def fix_ids_in_dir(base_dir, entity, id_col_suffix):
    print(f"Fixing IDs for {entity} in {base_dir}...")

    # Collect monthly files for this entity
    files = [f for f in os.listdir(base_dir) if f.endswith(f"_{entity}.csv")]
    files.sort()  # chronological order

    current_id = 1
    for file in files:
        file_path = os.path.join(base_dir, file)

        # Load CSV
        df = pd.read_csv(file_path)

        # Find the ID column (e.g., "recording_id", "ivr_path_id", "event_id", "ticket_id")
        id_col = [col for col in df.columns if col.endswith(id_col_suffix)][0]

        # Assign unique IDs across all months
        new_ids = list(range(current_id, current_id + len(df)))
        df[id_col] = new_ids
        current_id += len(df)

        # Save back
        df.to_csv(file_path, index=False)

    print(f"✅ Finished fixing {entity}\n")


# Fix aux_by_month entities
for entity in aux_entities:
    suffix = "_id"  # all have id columns ending with _id
    fix_ids_in_dir(aux_dir, entity, suffix)

# Fix tickets_by_month
fix_ids_in_dir(tickets_dir, "tickets", "_id")
