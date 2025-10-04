"""
Unified fake data generator for a call-center schema.
- Writes base tables (agents, customers, skills, queues, etc.)
- Streams a full-year of calls (13,687,500 rows) into per-month CSVs
- Writes tickets (1 ticket per call) into per-month CSVs
- Generates extended tables (shifts, sampled recordings, IVR, SLAs, skill history, agent_workload, wrap codes, dispositions)
- Uses Faker for realistic names
- Multithreaded: generates each month in its own worker via ThreadPoolExecutor

OUT DIR (user-provided) :
out_dir = r'C:\\Users\\Mohamed Mohsin\\python tests\\MnHna\\CS\\data'
"""
import os
import csv
import random
import calendar
from datetime import datetime, timedelta, time, date
from faker import Faker
from concurrent.futures import ThreadPoolExecutor, as_completed

# -------------------------
# CONFIG
# -------------------------
out_dir = r'C:\\Users\\Mohamed Mohsin\\python tests\\MnHna\\CS\\data'
os.makedirs(out_dir, exist_ok=True)

# Year and call counts
YEAR = 2025
NUM_AGENTS = 150
NUM_CUSTOMERS = 5000
TOTAL_CALLS = 13687500           # user requested
DAYS_IN_YEAR = 365               # 2025 is not leap
CALLS_PER_DAY = TOTAL_CALLS // DAYS_IN_YEAR  # should be 37500
assert CALLS_PER_DAY * DAYS_IN_YEAR == TOTAL_CALLS, "TOTAL_CALLS must evenly divide 365"

# Sampling probabilities for auxiliary logs (tunable)
RECORDING_PROB = 0.4     # probability to produce a recording row for an answered call
IVR_PATH_PROB = 0.025    # probability a call has IVR path rows (sample)
CALL_EVENTS_PROB = 0.05  # probability to produce call_events for a call (sample)

# Schema sizes
NUM_SKILLS = 5
NUM_QUEUES = 6
NUM_CAMPAIGNS = 8
NUM_WRAP_CODES = 20
NUM_DISPOSITIONS = 12

RANDOM_SEED = 42
random.seed(RANDOM_SEED)
fake = Faker()
Faker.seed(RANDOM_SEED)

# Concurrency
MAX_WORKERS = 4  # adjust according to CPU / disk

# Helpers
def ensure_dir(p):
    if not os.path.exists(p):
        os.makedirs(p, exist_ok=True)

def daterange(start_date, end_date):
    d = start_date
    while d <= end_date:
        yield d
        d += timedelta(days=1)

# -------------------------
# 1) Small reference tables: skills, queues, campaigns, wrap_codes, dispositions
# -------------------------
print("Writing reference tables...")

# Skills - meaningful names
skills_list = [
    "Technical Support",
    "Sales",
    "Billing",
    "High Value Customer Handling",
    "Retention"
]
skills_csv = os.path.join(out_dir, "skills.csv")
with open(skills_csv, "w", newline='', encoding='utf-8') as f:
    w = csv.writer(f)
    w.writerow(["skill_id", "skill_name"])
    for sid, sname in enumerate(skills_list, start=1):
        w.writerow([sid, sname])

# Queues
queues = []
for qid in range(1, NUM_QUEUES+1):
    primary_skill = random.randint(1, NUM_SKILLS)
    queues.append((qid, f"Queue_{qid}", f"Queue {qid} description", primary_skill))
queues_csv = os.path.join(out_dir, "queues.csv")
with open(queues_csv, "w", newline='', encoding='utf-8') as f:
    w = csv.writer(f)
    w.writerow(["queue_id", "queue_name", "description", "primary_skill_id"])
    for row in queues:
        w.writerow(row)

# Campaigns
campaigns_csv = os.path.join(out_dir, "campaigns.csv")
with open(campaigns_csv, "w", newline='', encoding='utf-8') as f:
    w = csv.writer(f)
    w.writerow(["campaign_id", "campaign_name", "start_date", "end_date"])
    for cid in range(1, NUM_CAMPAIGNS+1):
        start = (datetime(YEAR-1, random.randint(1,12), random.randint(1,28))).date()
        end = None
        if random.random() < 0.6:
            end = (start + timedelta(days=random.randint(30, 365))).isoformat()
        w.writerow([cid, f"Campaign_{cid}", start.isoformat(), end])

# Wrap codes
wrap_codes_csv = os.path.join(out_dir, "wrap_codes.csv")
with open(wrap_codes_csv, "w", newline='', encoding='utf-8') as f:
    w = csv.writer(f)
    w.writerow(["wrap_code_id", "wrap_code", "description"])
    for i in range(1, NUM_WRAP_CODES+1):
        w.writerow([i, f"WRAP_{i}", f"Wrap reason {i}"])

# Dispositions
dispositions_list = ["Resolved","Escalated","Voicemail","Dropped","No Answer","Callback Requested",
                     "Survey Complete","Complaint","Refund","Information","Follow-up","Busy"]
dispositions_csv = os.path.join(out_dir, "dispositions.csv")
with open(dispositions_csv, "w", newline='', encoding='utf-8') as f:
    w = csv.writer(f)
    w.writerow(["disposition_id", "disposition"])
    for i, d in enumerate(dispositions_list, start=1):
        w.writerow([i, d])

# -------------------------
# 2) Agents and AgentSkills
# -------------------------
print("Generating agents and agent_skills...")

agents_csv = os.path.join(out_dir, "agents.csv")
with open(agents_csv, "w", newline='', encoding='utf-8') as f:
    w = csv.writer(f)
    w.writerow(["agent_id", "first_name", "last_name", "username", "phone", "email", "hire_date", "status"])
    for aid in range(1, NUM_AGENTS+1):
        fn = fake.first_name()
        ln = fake.last_name()
        username = (fn[0] + ln).lower() + str(aid % 100)
        phone = fake.phone_number()
        email = f"{fn.lower()}.{ln.lower()}{aid}@example.com"
        hire_date = (datetime(2016,1,1) + timedelta(days=random.randint(0, 365*9))).date().isoformat()
        status = random.choices(["Active","On Leave","Training","Inactive"], weights=[0.8,0.05,0.1,0.05])[0]
        w.writerow([aid, fn, ln, username, phone, email, hire_date, status])

agent_skills_csv = os.path.join(out_dir, "agent_skills.csv")
with open(agent_skills_csv, "w", newline='', encoding='utf-8') as f:
    w = csv.writer(f)
    w.writerow(["agent_id", "skill_id", "proficiency"])
    for aid in range(1, NUM_AGENTS+1):
        num = random.choices([1,2,3], weights=[0.6,0.3,0.1])[0]
        chosen = random.sample(range(1, NUM_SKILLS+1), num)
        for s in chosen:
            w.writerow([aid, s, random.randint(1,5)])

# -------------------------
# 3) Customers (big)
# -------------------------
print("Generating customers (this may take a moment)...")
customers_csv = os.path.join(out_dir, "customers.csv")
countries = ["US","CA","GB","AU","EG","FR","ES","DE"]

with open(customers_csv, "w", newline='', encoding='utf-8') as f:
    w = csv.writer(f)
    w.writerow(["customer_id", "first_name", "last_name", "email", "phone", "created_date", "country"])
    for cid in range(1, NUM_CUSTOMERS+1):
        fn = fake.first_name()
        ln = fake.last_name()
        email = fake.free_email()
        phone = fake.phone_number()
        created = (datetime(YEAR-5,1,1) + timedelta(days=random.randint(0, 365*5))).date().isoformat()
        country = random.choice(countries)
        w.writerow([cid, fn, ln, email, phone, created, country])

# -------------------------
# 4) Shifts (generate schedule for whole year)
# -------------------------
print("Generating shifts for the year (one row per agent per day pattern)...")
shifts_csv = os.path.join(out_dir, "shifts.csv")
with open(shifts_csv, "w", newline='', encoding='utf-8') as f:
    w = csv.writer(f)
    w.writerow(["shift_id", "agent_id", "shift_date", "start_time", "end_time", "shift_type"])
    sid = 1
    for aid in range(1, NUM_AGENTS+1):
        offset = aid % 7
        for d in daterange(date(YEAR,1,1), date(YEAR,12,31)):
            # define a 5-on/2-off rotating pattern
            if (d.toordinal() + offset) % 7 in (0,1,2,3,4):
                st = "09:00:00"
                et = "17:00:00"
                stype = "Morning"
            else:
                st = "17:00:00"
                et = "23:00:00"
                stype = "Evening"
            w.writerow([sid, aid, d.isoformat(), st, et, stype])
            sid += 1

# -------------------------
# 5) Service Levels (SLA)
# -------------------------
print("Generating Service Levels (SLA)...")
service_levels_csv = os.path.join(out_dir, "service_levels.csv")
with open(service_levels_csv, "w", newline='', encoding='utf-8') as f:
    w = csv.writer(f)
    w.writerow(["sla_id", "queue_id", "target_percentage", "target_seconds", "effective_date", "expiry_date"])
    sid = 1
    for qid in range(1, NUM_QUEUES+1):
        w.writerow([sid, qid, random.choice([75.00,80.00,85.00,90.00]), random.choice([20,30,45]), f"{YEAR}-01-01", ""])
        sid += 1

# -------------------------
# 6) SkillHistory (sampled)
# -------------------------
print("Generating skill history (sampled)...")
skill_history_csv = os.path.join(out_dir, "skill_history.csv")
with open(skill_history_csv, "w", newline='', encoding='utf-8') as f:
    w = csv.writer(f)
    w.writerow(["history_id", "agent_id", "skill_id", "proficiency", "effective_date", "end_date"])
    hid = 1
    sampled_agents = random.sample(range(1, NUM_AGENTS+1), k=int(NUM_AGENTS*0.3))
    for aid in sampled_agents:
        num = random.choice([1,2,3])
        base = datetime(YEAR-3,1,1)
        for _ in range(num):
            sid = random.randint(1, NUM_SKILLS)
            prof = random.randint(1,5)
            eff = (base + timedelta(days=random.randint(0, 365))).date().isoformat()
            w.writerow([hid, aid, sid, prof, eff, ""])
            hid += 1

# -------------------------
# 7) AgentWorkload (sampled)
# -------------------------
print("Generating agent workload (sampled)...")
agent_workload_csv = os.path.join(out_dir, "agent_workload.csv")
with open(agent_workload_csv, "w", newline='', encoding='utf-8') as f:
    w = csv.writer(f)
    w.writerow(["workload_id", "agent_id", "date", "max_concurrent_calls", "assigned_calls"])
    wid = 1
    sampled = random.sample(range(1, NUM_AGENTS+1), k=int(NUM_AGENTS*0.25))
    for aid in sampled:
        for d in daterange(date(YEAR,1,1), date(YEAR,1,7)):
            w.writerow([wid, aid, d.isoformat(), random.choice([1,2,3]), random.randint(0,5)])
            wid += 1

# -------------------------
# Prepare month-by-month generation plan
# -------------------------
print("Preparing monthly generation plan...")
months_info = []
cumulative = 0
for m in range(1, 13):
    days_in_month = calendar.monthrange(YEAR, m)[1]
    calls_in_month = CALLS_PER_DAY * days_in_month
    start_id = cumulative + 1
    end_id = cumulative + calls_in_month
    months_info.append({
        "month": m,
        "month_name": datetime(YEAR, m, 1).strftime("%B"),
        "days_in_month": days_in_month,
        "calls_in_month": calls_in_month,
        "start_call_id": start_id,
        "end_call_id": end_id
    })
    cumulative = end_id

# sanity
assert cumulative == TOTAL_CALLS, "Monthly totals do not sum to TOTAL_CALLS"

# Create per-month dirs
calls_month_dir = os.path.join(out_dir, "calls_by_month")
tickets_month_dir = os.path.join(out_dir, "tickets_by_month")
aux_month_dir = os.path.join(out_dir, "aux_by_month")
ensure_dir(calls_month_dir)
ensure_dir(tickets_month_dir)
ensure_dir(aux_month_dir)

# small sets for joins
agent_ids = list(range(1, NUM_AGENTS+1))
customer_ids = list(range(1, NUM_CUSTOMERS+1))
queue_ids = [q[0] for q in queues]
campaign_ids = list(range(1, NUM_CAMPAIGNS+1))
wrap_code_ids = list(range(1, NUM_WRAP_CODES+1))
disposition_ids = list(range(1, NUM_DISPOSITIONS+1))

# hour distribution to bias peak hours
hour_weights = [0.5]*6 + [1.0]*6 + [1.5]*6 + [1.2]*6  # total 24
hour_choices = []
for h, wght in enumerate(hour_weights):
    reps = max(1, int(wght * 10))
    hour_choices.extend([h]*reps)

# -------------------------
# Function to generate a single month (worker)
# -------------------------
def generate_month_worker(info):
    m = info["month"]
    name = info["month_name"]
    days_in_month = info["days_in_month"]
    calls_in_month = info["calls_in_month"]
    start_call_id = info["start_call_id"]

    print(f"[START] Month {name}: calls={calls_in_month}, start_id={start_call_id}")

    # File paths
    calls_file = os.path.join(calls_month_dir, f"{name}_calls.csv")
    tickets_file = os.path.join(tickets_month_dir, f"{name}_tickets.csv")
    rec_file = os.path.join(aux_month_dir, f"{name}_recordings.csv")
    ivr_file = os.path.join(aux_month_dir, f"{name}_ivr_paths.csv")
    events_file = os.path.join(aux_month_dir, f"{name}_call_events.csv")

    # Open files (each month worker writes its own files -> thread-safe)
    with open(calls_file, "w", newline='', encoding='utf-8') as cf, \
         open(tickets_file, "w", newline='', encoding='utf-8') as tf, \
         open(rec_file, "w", newline='', encoding='utf-8') as rf, \
         open(ivr_file, "w", newline='', encoding='utf-8') as ivf, \
         open(events_file, "w", newline='', encoding='utf-8') as ef:

        call_writer = csv.writer(cf)
        ticket_writer = csv.writer(tf)
        rec_writer = csv.writer(rf)
        ivr_writer = csv.writer(ivf)
        ev_writer = csv.writer(ef)

        # Headers
        call_writer.writerow([
            "call_id","call_timestamp","queue_id","campaign_id","customer_id","answered",
            "wait_seconds","talk_seconds","hold_seconds","agent_id","transferred_to_agent_id",
            "wrap_code_id","disposition_id","recording_path","survey_id","survey_rating"
        ])
        ticket_writer.writerow([
            "ticket_id","call_id","customer_id","created_at","status","priority","subject","description"
        ])
        rec_writer.writerow(["recording_id","call_id","file_path","file_size_kb","duration_seconds","transcription_status"])
        ivr_writer.writerow(["path_id","call_id","node_id","dtmf_input","timestamp"])
        ev_writer.writerow(["event_id","call_id","event_time","event_type","agent_id","from_agent_id","to_agent_id"])

        # counters local to month
        call_local_id = start_call_id - 1
        ticket_local_id = 0
        recording_local_id = 0
        ivr_local_id = 0
        event_local_id = 0

        # generate day by day for consistent per-day CALLS_PER_DAY
        for d in range(1, days_in_month + 1):
            current_day = date(YEAR, m, d)
            for i in range(CALLS_PER_DAY):
                call_local_id += 1
                ticket_local_id += 1

                # timestamp (biased hours)
                hour = random.choice(hour_choices)
                minute = random.randint(0,59)
                second = random.randint(0,59)
                call_ts = datetime.combine(current_day, time(hour, minute, second))

                queue_id = random.choice(queue_ids)
                campaign_id = random.choice(campaign_ids + [None]*3)  # some calls not from campaigns
                customer_id = random.choice(customer_ids)

                answered_prob = 0.88 if queue_id % 2 == 0 else 0.82
                answered = 1 if random.random() < answered_prob else 0

                wait_seconds = 0
                talk_seconds = 0
                hold_seconds = 0
                agent_id = ""
                transferred_to_agent = ""
                wrap_code = ""
                disposition = ""
                recording_path = ""
                survey_id = ""
                survey_rating = ""

                if answered:
                    agent_id = random.choice(agent_ids)
                    wait_seconds = max(0, int(random.expovariate(1/20)))
                    talk_seconds = random.randint(20, 3600)
                    hold_seconds = int(talk_seconds * random.random() * 0.2) if random.random() < 0.25 else 0
                    if random.random() < 0.08:
                        transferred_to_agent = random.choice([a for a in agent_ids if a != agent_id])
                    wrap_code = random.choice(wrap_code_ids)
                    disposition = random.choice(disposition_ids)
                    if random.random() < 0.18:
                        survey_id = random.randint(1, 20000000)
                        survey_rating = random.randint(1,5)

                    # Recordings (sampled)
                    if random.random() < RECORDING_PROB:
                        recording_local_id += 1
                        recording_path = f"/recordings/{call_ts.date().isoformat()}/call_{call_local_id}.wav"
                        file_size_kb = int(talk_seconds * random.uniform(8,20))
                        rec_writer.writerow([recording_local_id, call_local_id, recording_path, file_size_kb, talk_seconds, random.choice(["Pending","Completed","Failed"])])

                    # IVR paths (sampled)
                    if random.random() < IVR_PATH_PROB:
                        ivr_local_id += 1
                        ivr_writer.writerow([ivr_local_id, call_local_id, 1, str(random.choice([1,2])), call_ts.isoformat()])
                        if random.random() < 0.6:
                            ivr_local_id += 1
                            ivr_writer.writerow([ivr_local_id, call_local_id, random.choice([2,3]), str(random.choice([1,2])), (call_ts + timedelta(seconds=4)).isoformat()])

                    # Call events (sampled)
                    if random.random() < CALL_EVENTS_PROB:
                        event_local_id += 1
                        ev_writer.writerow([event_local_id, call_local_id, call_ts.isoformat(), "queued", "", "", ""])
                        event_local_id += 1
                        ev_writer.writerow([event_local_id, call_local_id, (call_ts + timedelta(seconds=wait_seconds)).isoformat(), "answered", agent_id, "", ""])
                        if hold_seconds > 0:
                            event_local_id += 1
                            ev_writer.writerow([event_local_id, call_local_id, (call_ts + timedelta(seconds=wait_seconds + int(talk_seconds*0.2))).isoformat(), "hold", agent_id, "", ""])
                            event_local_id += 1
                            ev_writer.writerow([event_local_id, call_local_id, (call_ts + timedelta(seconds=wait_seconds + int(talk_seconds*0.2) + hold_seconds)).isoformat(), "resumed", agent_id, "", ""])
                        event_local_id += 1
                        ev_writer.writerow([event_local_id, call_local_id, (call_ts + timedelta(seconds=wait_seconds + talk_seconds)).isoformat(), "ended", agent_id, "", ""])

                # prepare call row and write
                call_writer.writerow([
                    call_local_id,
                    call_ts.isoformat(),
                    queue_id,
                    campaign_id if campaign_id is not None else "",
                    customer_id,
                    answered,
                    wait_seconds,
                    talk_seconds,
                    hold_seconds,
                    agent_id if agent_id != "" else "",
                    transferred_to_agent if transferred_to_agent != "" else "",
                    wrap_code if wrap_code != "" else "",
                    disposition if disposition != "" else "",
                    recording_path if recording_path != "" else "",
                    survey_id if survey_id != "" else "",
                    survey_rating if survey_rating != "" else ""
                ])

                # ticket one per call
                t_status = random.choice(["Open","In Progress","Resolved","Closed","Escalated"])
                t_priority = random.choice(["Low","Medium","High","Critical"]) if answered and talk_seconds > 600 else random.choice(["Low","Medium"])
                t_subject = f"Ticket for call {call_local_id}"
                t_desc = f"Auto ticket for call {call_local_id} created on {call_ts.isoformat()}."
                ticket_writer.writerow([
                    ticket_local_id,
                    call_local_id,
                    customer_id,
                    call_ts.isoformat(),
                    t_status,
                    t_priority,
                    t_subject,
                    t_desc
                ])

            # optional flush per day to reduce memory buffer
            cf.flush()
            tf.flush()
            rf.flush()
            ivf.flush()
            ef.flush()

    # done for month
    print(f"[DONE ] Month {name}: generated calls {start_call_id}..{call_local_id} (count={call_local_id - start_call_id + 1})")
    return {"month": m, "month_name": name, "start": start_call_id, "end": call_local_id}

# -------------------------
# Run monthly workers in thread pool
# -------------------------
print(f"Starting multithreaded generation with max_workers={MAX_WORKERS} ...")
results = []
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    future_to_month = {executor.submit(generate_month_worker, mi): mi for mi in months_info}
    for future in as_completed(future_to_month):
        mi = future_to_month[future]
        try:
            res = future.result()
            results.append(res)
        except Exception as exc:
            print(f"[ERROR] Month {mi['month_name']} generated exception: {exc}")

# -------------------------
# Finalize: small summaries & finish
# -------------------------
print("Writing calls_summary_by_month.csv ...")
summary_csv = os.path.join(out_dir, "calls_summary_by_month.csv")
with open(summary_csv, "w", newline='', encoding='utf-8') as f:
    w = csv.writer(f)
    w.writerow(["month","calls_generated"])
    for mi in months_info:
        w.writerow([mi["month_name"], mi["calls_in_month"]])

print("Generator finished. Output directory:", out_dir)
print("Calls by month:", os.path.join(out_dir, "calls_by_month"))
print("Tickets by month:", os.path.join(out_dir, "tickets_by_month"))
print("Auxiliary per-month files (recordings/ivr/events):", os.path.join(out_dir, "aux_by_month"))
