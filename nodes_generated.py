import os
import pandas as pd

# Base directory for metadata
out_dir = r"C:\Users\Mohamed Mohsin\python tests\MnHna\CS\data"

# Define IVR Menu Nodes (hierarchy)
ivr_nodes = [
    {"node_id": 1, "parent_node_id": None, "menu_text": "Welcome to our call center. Please choose an option.", "action_": "play_menu"},
    {"node_id": 2, "parent_node_id": 1, "menu_text": "For Sales, press 1", "action_": "goto_sales"},
    {"node_id": 3, "parent_node_id": 1, "menu_text": "For Support, press 2", "action_": "goto_support"},
    {"node_id": 4, "parent_node_id": 1, "menu_text": "For Billing, press 3", "action_": "goto_billing"},

    # Sales submenu
    {"node_id": 5, "parent_node_id": 2, "menu_text": "New Orders", "action_": "transfer_sales_new"},
    {"node_id": 6, "parent_node_id": 2, "menu_text": "Existing Orders", "action_": "transfer_sales_existing"},

    # Support submenu
    {"node_id": 7, "parent_node_id": 3, "menu_text": "Technical Issues", "action_": "transfer_support_tech"},
    {"node_id": 8, "parent_node_id": 3, "menu_text": "Account Issues", "action_": "transfer_support_account"},

    # Billing submenu
    {"node_id": 9, "parent_node_id": 4, "menu_text": "Invoice Questions", "action_": "transfer_billing_invoice"},
    {"node_id": 10, "parent_node_id": 4, "menu_text": "Payment Issues", "action_": "transfer_billing_payment"},
]

# Create DataFrame
df_nodes = pd.DataFrame(ivr_nodes)

# Save to CSV
out_path = os.path.join(out_dir, "ivr_menu_nodes.csv")
df_nodes.to_csv(out_path, index=False)

print(f"✅ IVR menu nodes generated at: {out_path}")
