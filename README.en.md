# ISEKAI Compute

**Process sensitive data without ever exposing it.**

ISEKAI Compute is a secure computation platform built on Trusted Execution Environments (TEE), enabling data processing under strict policy control without exposing raw data.

---

## 🚀 Key Features

- 🔒 **Data never leaves its owner**
- 🧠 **Execute computation in trusted enclaves (TEE)**
- 📜 **Policy-enforced data usage**
- 🔗 **Cross-organization data collaboration without sharing data**

---

## 🧩 Problem

In domains such as healthcare and finance, there is a fundamental tension:

- Sensitive data cannot be shared due to privacy regulations
- Valuable insights require large-scale data analysis
- Trust between organizations is limited

How can we use data **without exposing it**?

---

## 💡 Solution

ISEKAI Compute introduces a new model:

> **Data stays with the owner. Only computation results are shared.**

- Computation is executed inside a TEE
- Data is fetched only when needed
- Policies define what computation is allowed
- Raw data is never exposed outside the trusted environment

---

## 🏗 Architecture
![ISEKAI Compute Architecture](./docs/ISEKAI_arch.png)


### 🧩 Module Model

ISEKAI Compute executes user-defined modules written in WebAssembly (WASM).

This provides:
- sandboxed execution
- language-agnostic extensibility
- safe deployment inside TEE

Modules include:
- Data Access Modules (fetch data from data servers)
- Compute Modules (e.g. pseudonymization, aggregation)

---

### Flow

1. User submits computation (DAG)
2. ISEKAI requests data from data servers
3. Data is processed inside TEE only
4. Policy-controlled computation runs
5. Only results are returned to the user

---

## 🧩 Use Case: Medical Data Processing

### Problem

Medical data (e.g., EHRs) is highly sensitive:

- Hospitals cannot share raw data
- Researchers need access to large datasets
- Cross-hospital analysis is difficult

---

### Solution

ISEKAI enables secure medical data processing:

- Hospitals operate ISEKAI Data Servers
- Data remains inside hospital infrastructure
- Computation runs in TEE (ISEKAI Compute)
- Only processed results are shared

---

### Key Benefits

- 🔒 Only hospitals can access raw medical records
- 🚫 Even platform operators cannot view the data
- 🧪 Enables secure pseudonymization
- 🔗 Enables cross-hospital data collaboration

---

### Advanced Capability

ISEKAI can access multiple hospital databases simultaneously and process them as a unified dataset.

This allows:

- Cross-hospital analytics
- Accurate entity matching
- Higher-quality medical research and drug development

---

## 🔐 Security Model

ISEKAI Compute is based on Trusted Execution Environments (TEE):

- Data and code are protected even from system administrators
- Memory is encrypted and isolated
- Only attested code can execute

Additionally:

- All data access is policy-controlled
- Outputs can be restricted (e.g., aggregation, anonymization)

---

## 📜 Data Policy Model

Each dataset is associated with a policy defining:

- Allowed computations
- Required transformations (e.g., pseudonymization)
- Output constraints

**Computation is enforced by policy, not trust.**

---

## 📄 Background

In domains such as healthcare and finance, protecting personal data while enabling its utilization is a major challenge.  
The concept of **data control rights**—where data owners control how their data is used—is gaining attention.

ISEKAI Compute realizes this concept by ensuring:

- Only permitted computation is executed
- Data usage is strictly controlled
- Raw data is never exposed

In the medical domain, ISEKAI enables transformation of hospital-held medical records into pseudonymized datasets usable for research and pharmaceutical development, without exposing the original data.

---

## 🔧 Getting Started

### QuickStart
- If you are trying it for the first time, please read [QuickStart](QuickStart.en.md).

---

### ISEKAI Data Server
- [ISEKAI Data Server](isekai-data-server) is a server that provides data to ISEKAI Computation.
It can also run on a client machine behind NAT.

---

### ISEKAI Data Provider Module
- [ISEKAI Data Provider Module](mods/mod-http-data-flight) runs on ISEKAI Computation and retrieves data by connecting to the ISEKAI Data Server.

---

### ISEKAI Computation Module
- The ISEKAI Computation Module runs on ISEKAI Computation and performs various processing tasks such as calculating averages.
- It is not publicly available at this time.
