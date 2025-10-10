CREATE DATABASE CALL_CENTER_DB;
GO

USE CALL_CENTER_DB;
GO



-- ==============================
-- Call Center Database Schema
-- ==============================

-- Customers - DONE
CREATE TABLE Customers (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    phone_number VARCHAR(50),
    email VARCHAR(100),
	created_date date,
	country VARCHAR(50)
);

-- Agents - DONE
CREATE TABLE Agents (
    agent_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
	UserName VARCHAR(50),
	PHONE VARCHAR(50),
	Email VARCHAR(100),
    hire_date DATE,
    status VARCHAR(50)
);

-- Queues - DONE
CREATE TABLE Queues (
    queue_id INT PRIMARY KEY,
    queue_name VARCHAR(50),
    description VARCHAR(200),
	PRIMARY_SKILL_ID VARCHAR(50)
);

-- Skills - DONE
CREATE TABLE Skills (
    skill_id INT PRIMARY KEY,
    skill_name VARCHAR(50)
);

-- Agent Skills - DONE
CREATE TABLE Agent_Skills (
    agent_id INT NOT NULL,
    skill_id INT NOT NULL,
    proficiency_level INT,
    PRIMARY KEY (agent_id, skill_id),
    FOREIGN KEY (agent_id) REFERENCES Agents(agent_id),
    FOREIGN KEY (skill_id) REFERENCES Skills(skill_id)
);

-- Calls - DONE FINALLYYYYYYYYYY
CREATE TABLE Calls (
    call_id BIGINT PRIMARY KEY,
    customer_id INT NOT NULL,
    agent_id INT NULL,
    queue_id INT NOT NULL,
	campaign_id INT,
    call_time DATETIME2,
	answered INT,
	wait_seconds INT,
	talk_seconds INT,
	hold_seconds INT,
    --duration INT,
    --outcome VARCHAR(50),
	transferred_to_agent_id VARCHAR(50),
	wrap_code_id INT,
	disposition_id INT,
	recording_path VARCHAR(300),
	survey_id INT,
	survey_rating INT,
    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id),
    FOREIGN KEY (agent_id) REFERENCES Agents(agent_id),
    FOREIGN KEY (queue_id) REFERENCES Queues(queue_id)
);

-- Shifts - DONE
CREATE TABLE Shifts (
    shift_id INT PRIMARY KEY,
    agent_id INT NOT NULL,
    shift_date DATE,
    shift_start TIME,
    shift_end TIME,
    shift_type VARCHAR(50),
    FOREIGN KEY (agent_id) REFERENCES Agents(agent_id)
);

-- Recordings
CREATE TABLE Recordings (
    recording_id BIGINT,
    call_id BIGINT NOT NULL,
    file_path VARCHAR(200),
    file_size_kb INT,
	duration_seconds INT,
    transcription_status VARCHAR(50),
    FOREIGN KEY (call_id) REFERENCES Calls(call_id)
);



-- Tickets
CREATE TABLE Tickets (
    ticket_id BIGINT /*PRIMARY KEY*/,
    call_id BIGINT NOT NULL,
    customer_id INT NOT NULL,
    status_ VARCHAR(50),
    priority_ VARCHAR(50),
    created_at DATETIME,
	SUBJECT_ VARCHAR(50),
	Description VARCHAR(MAX)--,
    --FOREIGN KEY (call_id) REFERENCES Calls(call_id),
    --FOREIGN KEY (customer_id) REFERENCES Customers(customer_id)
);

-- IVR Menu Nodes
CREATE TABLE IVR_Menu_Nodes (
    node_id INT PRIMARY KEY,
    parent_node_id INT NULL,
    menu_text VARCHAR(400),
    action_ VARCHAR(100)
    -- FOREIGN KEY (parent_node_id) REFERENCES IVR_Menu_Nodes(node_id)
);

-- IVR Paths
CREATE TABLE IVR_Paths (
    path_id BIGINT /*PRIMARY KEY*/,
    call_id BIGINT NOT NULL,
    node_id INT NOT NULL,
    dtmf_input INT,
	timestamp_ DATETIME2,
    FOREIGN KEY (call_id) REFERENCES Calls(call_id),
    FOREIGN KEY (node_id) REFERENCES IVR_Menu_Nodes(node_id)
);

-- Service Levels (SLAs)
CREATE TABLE Service_Levels (
    sla_id INT PRIMARY KEY,
    queue_id INT NOT NULL,
    target_percentage DECIMAL(5,2),
    target_time_seconds INT,
	effective_date DATE,
	expiry_date DATE
    FOREIGN KEY (queue_id) REFERENCES Queues(queue_id)
);


-- Skill Proficiency History
CREATE TABLE Skill_Proficiency_History (
    history_id INT PRIMARY KEY,
    agent_id INT NOT NULL,
    skill_id INT NOT NULL,
    proficiency_level VARCHAR(50),
    effective_date DATE,
	end_date DATE,
    FOREIGN KEY (agent_id) REFERENCES Agents(agent_id),
    FOREIGN KEY (skill_id) REFERENCES Skills(skill_id)
);

-- Agent Workload
CREATE TABLE Agent_Workload (
    workload_id BIGINT PRIMARY KEY,
    agent_id INT NOT NULL,
    date DATE,
    max_concurrent_calls INT,
    assigned_calls INT,
    FOREIGN KEY (agent_id) REFERENCES Agents(agent_id)
);









