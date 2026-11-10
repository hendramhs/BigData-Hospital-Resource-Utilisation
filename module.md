ETS IFB-451 BIG DATA
MID-TERM GROUP ASSIGNMENT
5-Minute Big Data Discovery and Demonstration Video
A Healthcare Informatics Approach Using the 5Vs of Big Data
Academic Year: 2026/2027
Programme: Bachelor of Informatics
Department: Informatics Department, Institut Teknologi Nasional (Itenas)
Bandung
Koordinator Pengampu:
Dr Edi Nuryatno, PhD
Classes AA and BB
Lecturer:
Dr Saripudin
Class CC
Prepared by:
Dr Edi Nuryatno
PhD, MSc, MACS CT, BSc
IP3P (Int.), Cert. ICH GCP (Aus.), Cert. SKT (Aus.), Cert. CCI (Aus.), Cert. GCP (Aus.), Cert.
AI Teach. (Int.)
1. Assignment Overview
Component Description
Course IFB-451 BIG DATA
Assessment Mid-Term Group Assignment (ETS)
Assessment Weight 35%
Group Size 5 students
Learning Modules Modules 1–7
Student Competencies SC1, SC2 and SC3
Presentation Format 5-minute YouTube video
Presentation Approach Project & Presentation (P&P)
IFB-451 BIG DATA-Midterm Assignment-DrEdiNuryatno__2026 2 | P a g e

Component Description
Primary Focus Practical demonstration of the 5Vs of Big Data
Application Domain Healthcare and Public Health Informatics
Technical Requirement Functional Big Data pipeline prototype
Deliverables Video demonstration and three-page PDF
Submission Platform Itenas eLearning
Submission Deadline 10 November 2026
2. Purpose
This assignment examines the application of the five characteristics of Big
Data—Volume, Velocity, Variety, Veracity and Value—to healthcare
information processing. Working in groups of five, students will investigate a
healthcare problem and develop a functional prototype using the concepts
and technologies introduced in Modules 1–7.
Healthcare datasets present challenges beyond the quantity of information
they contain. Clinical records may involve complex relationships, repeated
observations, different data representations and demanding processing
requirements. Students are expected to examine these characteristics, justify
their technical approach and demonstrate how the resulting information
contributes to understanding the selected healthcare problem.
Central Assignment Question
How do the 5Vs influence the design and implementation of a healthcare data-
processing pipeline, and what meaningful healthcare information can be
obtained from it?
The five-minute presentation must address this question through evidence from
the group's working prototype.
3. The 5Vs: Core Demonstration Requirements
The 5Vs provide the basis for examining the selected dataset and evaluating
the proposed technical solution. Each characteristic must be demonstrated
through the group's implementation.
IFB-451 BIG DATA-Midterm Assignment-DrEdiNuryatno__2026 3 | P a g e

Table 3.1. The 5Vs: Conceptual Understanding and Technical Demonstration
| Characteristic  | Conceptual     | Healthcare  | Required       | Expected  |
| --------------- | -------------- | ----------- | -------------- | --------- |
|                 | Understanding  | Context     | Demonstration  | Evidence  |
Volume  Quantity of data  Longitudinal  Examine data  Record counts,
|     | generated,  | EHRs contain  | quantity,       | structure,      |
| --- | ----------- | ------------- | --------------- | --------------- |
|     | stored and  | encounters,   | structure and   | processing      |
|     | processed.  | diagnoses,    | relationships;  | operations and  |
|     | Structural  | medications   | demonstrate     | measured        |
relationships and  and laboratory  aggregation or  performance.
|     | computational   | observations  | relational   |     |
| --- | --------------- | ------------- | ------------ | --- |
|     | demands affect  | that may      | processing.  |     |
|     | processing but  | require       |              |     |
|     | are not         | complex       |              |     |
|     | themselves      | relational    |              |     |
|     | measures of     | processing.   |              |     |
Volume.
| Velocity  | Rate of data    | Monitoring      | Demonstrate        | Arrival        |
| --------- | --------------- | --------------- | ------------------ | -------------- |
|           | generation and  | systems may     | periodic or        | timestamps,    |
|           | arrival,        | produce         | simulated          | update         |
|           | considered      | observations    | continuous         | frequency and  |
|           | alongside       | every few       | arrivals; measure  | latency.       |
|           | required        | seconds;        | ingestion          |                |
|           | processing      | admission       | frequency and      |                |
|           | timeliness.     | records may be  | processing         |                |
|           |                 | updated         | latency.           |                |
periodically.
Variety  Differences in  Clinical  Process and  Source schemas,
data sources,  encounters,  integrate at least  transformations
|     | formats,          | laboratory   | two formats or    | and integrated  |
| --- | ----------------- | ------------ | ----------------- | --------------- |
|     | structures and    | results and  | representations.  | records.        |
|     | representations.  | monitoring   |                   |                 |
observations
may use
different
schemas.
Veracity  Accuracy,  Records may  Identify at least  Before-and-after
completeness,  contain missing  two quality  quality measures
|     | consistency and       | values,       | problems and   | and         |
| --- | --------------------- | ------------- | -------------- | ----------- |
|     | reliability of data.  | duplicates,   | apply          | documented  |
|     |                       | inconsistent  | appropriate    | handling    |
|     |                       | units or      | validation or  | decisions.  |
|     |                       | incorrect     | correction     |             |
|     |                       | timestamps.   | procedures.    |             |
Value  Usefulness of the  Admission  Answer a  Query results,
|     | information     | patterns may     | defined            | visualisation and  |
| --- | --------------- | ---------------- | ------------------ | ------------------ |
|     | produced        | inform capacity  | healthcare         | healthcare         |
|     | through         | planning;        | question and       | interpretation.    |
|     | processing and  | surveillance     | explain the        |                    |
|     | analysis.       | records may      | results and their  |                    |
|     |                 | reveal changes   | limitations.       |                    |
in reported
incidence.

IFB-451 BIG DATA-Midterm Assignment-DrEdiNuryatno__2026                                     4 | P a g e

Table 3.2. Minimum Demonstration Requirements and Assessment
Considerations
| Characteristic  |     | Acceptable Evidence       |     | Insufficient Evidence      |
| --------------- | --- | ------------------------- | --- | -------------------------- |
| Volume          |     | Demonstrated data         |     | File size or record count  |
|                 |     | scale, relationships and  |     | without processing         |
|                 |     | processing requirements.  |     | analysis.                  |
| Velocity        |     | Measured arrival          |     | Query execution time       |
|                 |     | frequency, ingestion and  |     | alone.                     |
processing latency.
| Variety  |     | Integration of two    |     | A list of formats without  |
| -------- | --- | --------------------- | --- | -------------------------- |
|          |     | different formats or  |     | integration.               |
representations.
| Veracity  |     | Identified quality issues,  |     | An unsupported claim    |
| --------- | --- | --------------------------- | --- | ----------------------- |
|           |     | justified procedures and    |     | that data was cleaned.  |
measured results.
| Value  |     | Findings interpreted in  |     | Charts without   |
| ------ | --- | ------------------------ | --- | ---------------- |
|        |     | relation to the          |     | interpretation.  |
healthcare problem.

The selected dataset must provide sufficient scope to investigate all five
characteristics. A large dataset is not compulsory, but students must explain the
technical challenges presented by their data and demonstrate how these are
addressed.
The assessment considers the appropriateness of the technical approach and
the evidence presented, not the size of the dataset or complexity of the software
alone.

4. Learning Outcomes and Module Alignment
The assessment covers Modules 1–7 and their corresponding Student
Competencies.
| SC  Modules  | Learning Focus  |     | Expected Demonstration  |     |
| ------------ | --------------- | --- | ----------------------- | --- |
SC1  M1–M2  Big Data concepts,  Analyse the 5Vs and justify the
characteristics, ecosystem  selected processing architecture.
and architecture.
| SC2  M3–M4  | Data storage,      |     | Demonstrate data storage,           |     |
| ----------- | ------------------ | --- | ----------------------------------- | --- |
|             | management and     |     | integration, structural complexity  |     |
|             | Hadoop ecosystem.  |     | and quality management.             |     |
IFB-451 BIG DATA-Midterm Assignment-DrEdiNuryatno__2026                                     5 | P a g e

| SC  Modules  | Learning Focus  |     | Expected Demonstration  |     |     |
| ------------ | --------------- | --- | ----------------------- | --- | --- |
SC3  M5–M7  MapReduce, Spark, Spark  Implement data processing
|     | SQL and DataFrames.  |     | operations, evaluate  |     |     |
| --- | -------------------- | --- | --------------------- | --- | --- |
performance and produce
analytical findings.

The modules should be presented as connected components of a single data
processing workflow.

5. Healthcare Case Study Selection
Each group must select one healthcare or public health case study and use it
consistently throughout the five-minute demonstration.
The following cases provide opportunities to examine different data structures,
processing requirements and healthcare applications. All five characteristics
must be addressed, regardless of the selected topic.
Table 5.1. Healthcare Case Studies Mapped to the 5Vs
| Case Study  | Volume  | Velocity  | Variety  | Veracity  | Value  |
| ----------- | ------- | --------- | -------- | --------- | ------ |
Longitudinal  Encounters  Periodic  Encounter and  Duplicates and  Healthcare
Electronic  and linked  encounter  JSON  missing  utilisation
Health Record  clinical  updates.  observation  observations.  patterns.
| Analytics  | observations.  |     | records.  |     |     |
| ---------- | -------------- | --- | --------- | --- | --- |
Hospital  Admissions  Admission  Admissions  Missing times  Demand
| Resource  | and  | and  | and bed- | and  | and  |
| --------- | ---- | ---- | -------- | ---- | ---- |
Utilisation  occupancy  discharge  capacity  inconsistent  capacity
|     | calculations.  | updates.  | records.  | occupancy.  | planning.  |
| --- | -------------- | --------- | --------- | ----------- | ---------- |
COVID-19  Notifications  Daily or  Cases,  Delays,  Reported
Epidemiological  by region  weekly  vaccination  duplicates and  infection
Analytics  and period.  reporting.  and  missing regions.  trends.
population
data.
Clinical  Repeated  Incoming  Laboratory  Missing results  Laboratory
Laboratory Data  tests and  result  and encounter  and  utilisation.
| Analytics  | related      | batches.  | records.  | inconsistent  |     |
| ---------- | ------------ | --------- | --------- | ------------- | --- |
|            | encounters.  |           |           | units.        |     |
Population  Disease  Periodic  Reports,  Incomplete  Regional
Health  reports  surveillance  demographics  reports and  disease
Surveillance  across  updates.  and  inconsistent  patterns.
|     | regions and  |     | geography.  | dates.  |     |
| --- | ------------ | --- | ----------- | ------- | --- |
groups.
Healthcare  Claims linked  Periodic  Claims,  Duplicate  Unusual
Insurance  to providers  claims  providers and  claims and  claims
Claims  and services.  submissions.  service  billing  patterns.
| Analytics  |     |     | records.  | inconsistencies.  |     |
| ---------- | --- | --- | --------- | ----------------- | --- |
IFB-451 BIG DATA-Midterm Assignment-DrEdiNuryatno__2026                                     6 | P a g e

| Case Study  | Volume  | Velocity  | Variety  | Veracity  | Value  |
| ----------- | ------- | --------- | -------- | --------- | ------ |
IoT-Based  Repeated  Simulated  Heart rate,  Missing or  Monitoring
Patient  physiological  timed  SpO? and  implausible  trends.
Monitoring  time-series  observations.  blood pressure.  readings.
observations.
Chronic Disease  Longitudinal  Periodic  Measurements,  Missing values,  Follow-up
Population  diabetes or  follow-up  demographics  duplicates and  and disease
Analytics  hypertension  updates.  and  unit issues.  patterns.
|     | records.  |     | encounters.  |     |     |
| --- | --------- | --- | ------------ | --- | --- |

5.2 Case Study Selection Requirements
The selected case study must support the complete technical demonstration
described in Section 3 and the five-minute video structure.
Students may use public, synthetic or appropriately de-identified healthcare
datasets. Where necessary, simulated updates may be introduced to
demonstrate Velocity.
Alternative healthcare topics are acceptable provided they address all five Vs
and produce an interpretable analytical result.

6. Tools and Technical Environment
Students may select technologies appropriate to their case study and the
competencies assessed in this assignment.
Table 6.1. Suggested Tools and Technologies
| Category     | Suggested Technology  |     | Application                |     |     |
| ------------ | --------------------- | --- | -------------------------- | --- | --- |
| Distributed  | Hadoop HDFS           |     | Storage and management of  |     |     |
| Storage      |                       |     | distributed datasets.      |     |     |
Data Processing  Apache Spark / PySpark  Data transformations,
aggregations and processing.
| Query Processing  | Spark SQL / Hive  |     | Analytical queries and  |     |     |
| ----------------- | ----------------- | --- | ----------------------- | --- | --- |
relational operations.
| Programming  | Python  |     | Data preparation and  |     |     |
| ------------ | ------- | --- | --------------------- | --- | --- |
processing.
| Data Formats  | CSV, JSON, Parquet  |     | Representation and  |     |     |
| ------------- | ------------------- | --- | ------------------- | --- | --- |
integration of heterogeneous
data.
IFB-451 BIG DATA-Midterm Assignment-DrEdiNuryatno__2026                                     7 | P a g e

| Category      | Suggested Technology  |     | Application              |     |     |
| ------------- | --------------------- | --- | ------------------------ | --- | --- |
| Data Quality  | pandas / PySpark      |     | Validation and cleaning  |     |     |
procedures.
Visualisation  Plotly / Power BI /  Presentation of analytical
|              | Grafana               |     | results.                   |     |     |
| ------------ | --------------------- | --- | -------------------------- | --- | --- |
| Development  | Jupyter Notebook /    |     | Prototype development and  |     |     |
| Environment  | Google Colab / Local  |     | demonstration.             |     |     |
Spark

Technical Scope
Students are not required to implement every technology listed above.
A functioning PySpark-based prototype is acceptable where it supports the
required demonstrations.
Students should select technologies according to the characteristics of their
data and the processing operations required.
Where computational resources are limited, students may use synthetic
datasets, scaled experiments or simulated data arrival.
Simulated conditions must be identified clearly.

7. Five-Minute Video Structure and Student Roles
The presentation must involve all five group members and demonstrate a single
healthcare data processing workflow.
Table 7.1. Video Structure and Student Responsibilities
| Segment          | Module(s)  | Student  | Duration  | 5Vs      | Required       |
| ---------------- | ---------- | -------- | --------- | -------- | -------------- |
|                  |            | Role     |           | Focus    | Demonstration  |
| 1. Healthcare    | M1         | Project  | 0:45      | All 5Vs  | Introduce the  |
| Problem and      |            | Lead /   |           |          | healthcare     |
| Data             |            | Analyst  |           |          | problem,       |
| Characteristics  |            |          |           |          | dataset and    |
relevant
characteristics,
including data
scale and
complexity.
IFB-451 BIG DATA-Midterm Assignment-DrEdiNuryatno__2026                                     8 | P a g e

| Segment        | Module(s)  | Student   | Duration  | 5Vs       | Required       |
| -------------- | ---------- | --------- | --------- | --------- | -------------- |
|                |            | Role      |           | Focus     | Demonstration  |
| 2. Big Data    | M2         | System    | 0:45      | Volume    | Present the    |
| Architecture   |            | Engineer  |           | and       | architecture   |
| and Data Flow  |            |           |           | Velocity  | and explain    |
how it
addresses data
structure,
processing
requirements
and data
arrival.
| 3. Data          | M3–M4  | Data      | 1:00  | Variety   | Demonstrate    |
| ---------------- | ------ | --------- | ----- | --------- | -------------- |
| Storage,         |        | Engineer  |       | and       | data formats,  |
| Integration and  |        |           |       | Veracity  | integration,   |
| Quality          |        |           |       |           | structural     |
relationships
and data
validation.
| 4. Data         | M5–M6  | Big Data   | 1:15  | Volume    | Execute      |
| --------------- | ------ | ---------- | ----- | --------- | ------------ |
| Processing and  |        | Developer  |       | and       | processing   |
| Computational   |        |            |       | Velocity  | operations,  |
| Requirements    |        |            |       |           | demonstrate  |
computational
complexity
where relevant
and report
processing
performance.
| 5. Analytical   | M7  | Data     | 1:15  | Veracity  | Execute           |
| --------------- | --- | -------- | ----- | --------- | ----------------- |
| Results and     |     | Analyst  |       | and       | analytical        |
| Healthcare      |     |          |       | Value     | queries, present  |
| Interpretation  |     |          |       |           | findings and      |
explain their
healthcare
relevance and
limitations.
IFB-451 BIG DATA-Midterm Assignment-DrEdiNuryatno__2026                                     9 | P a g e

| Segment  | Module(s)  | Student     | Duration  | 5Vs      | Required       |
| -------- | ---------- | ----------- | --------- | -------- | -------------- |
|          |            | Role        |           | Focus    | Demonstration  |
| TOTAL    | M1–M7      | 5 students  | 5:00      | All 5Vs  | Integrated     |
technical
demonstration

Table 7.2. Relationship Between the 5Vs, Teaching Modules and Student
Responsibilities
| 5Vs  | Relevant  | Student Responsibility  |     |     | Video    |
| ---- | --------- | ----------------------- | --- | --- | -------- |
|      | Modules   |                         |     |     | Segment  |
Volume  M1, M2,  Examine data scale and complexity,  Segments 1,
|     | M5, M6  | justify processing requirements and  |     |     | 2 and 4  |
| --- | ------- | ------------------------------------ | --- | --- | -------- |
demonstrate relevant operations.
Velocity  M1, M2,  Explain data arrival characteristics and  Segments 2
|     | M5, M6  | demonstrate ingestion or processing  |     |     | and 4  |
| --- | ------- | ------------------------------------ | --- | --- | ------ |
timeliness.
| Variety  | M3, M4  | Demonstrate heterogeneous data  |     |     | Segment 3  |
| -------- | ------- | ------------------------------- | --- | --- | ---------- |
formats, schemas and integration
procedures.
Veracity  M3, M4,  Identify data quality problems, apply  Segments 3
|     | M7  | validation procedures and assess their  |     |     | and 5  |
| --- | --- | --------------------------------------- | --- | --- | ------ |
implications for analysis.
| Value  | M7  | Execute analytical queries, interpret  |     |     | Segment 5  |
| ------ | --- | -------------------------------------- | --- | --- | ---------- |
findings and establish their relevance to
the healthcare problem.

7.3 Presentation Requirements
The video must contain evidence of a working prototype.
Students should demonstrate relevant code execution, data transformations,
processing outputs and analytical results.
The five presentation segments must form one coherent demonstration.
Each student must contribute to the technical explanation according to their
assigned role.
The presentation must not exceed five minutes.
IFB-451 BIG DATA-Midterm Assignment-DrEdiNuryatno__2026                                     10 | P a g e

8. Integrated Healthcare Demonstration Example
Case Study: Longitudinal Electronic Health Record Analytics
A healthcare organisation wants to analyse patient encounters and clinical
observations to understand patterns of healthcare utilisation.
The dataset contains synthetic patient records, diagnoses, medications,
laboratory observations and encounter histories.
Although the dataset may be relatively small, its structure contains relationships
that require multiple transformations and analytical operations.
Table 8.1. Demonstration of the 5Vs
| 5V  | Demonstration  | Expected Evidence  |
| --- | -------------- | ------------------ |
Volume  Process patient encounters and  Record counts, dataset
related clinical observations using  relationships, query operations
multiple joins and transformations.  and processing measurements.
Velocity  Simulate periodic updates to  Update frequency, ingestion
|     | clinical encounter records.  | process and processing  |
| --- | ---------------------------- | ----------------------- |
latency.
| Variety  | Integrate CSV patient encounter  | Source schemas,                 |
| -------- | -------------------------------- | ------------------------------- |
|          | records with JSON clinical       | transformations and integrated  |
|          | observations.                    | output.                         |
Veracity  Identify missing observations and  Validation results and data
|     | duplicate encounters.  | quality statistics.  |
| --- | ---------------------- | -------------------- |
Value  Analyse healthcare utilisation  Analytical results and
|     | across encounters and reporting  | interpretation of service  |
| --- | -------------------------------- | -------------------------- |
|     | periods.                         | utilisation patterns.      |

8.2 Demonstrating Computational Complexity
Students may compare two processing operations.
Operation A — Simple Aggregation
Count patient encounters by department.
Operation B — Relational and Temporal Analysis
Integrate patient encounters, laboratory observations and diagnoses to
examine clinical activity over time.

IFB-451 BIG DATA-Midterm Assignment-DrEdiNuryatno__2026                                     11 | P a g e

Table 8.2. Comparison of Processing Operations
| Aspect  | Operation A: Simple  | Operation B: Relational and  |
| ------- | -------------------- | ---------------------------- |
|         | Aggregation          | Temporal Analysis            |
Data Structure  Single structured dataset.  Multiple interconnected
datasets.
Processing  Grouping and counting.  Joins, transformations and
| Operations  |     | temporal analysis.  |
| ----------- | --- | ------------------- |
Data Relationships  Limited.  Multiple clinical and temporal
relationships.
| Computational  | Primarily dependent on   | Influenced by joins,       |
| -------------- | ------------------------ | -------------------------- |
| Requirements   | record count and         | transformations, data      |
|                | aggregation operations.  | distribution and temporal  |
processing.
| Expected       | Encounter counts by  | Integrated analysis of     |
| -------------- | -------------------- | -------------------------- |
| Demonstration  | department.          | encounters, diagnoses and  |
laboratory observations.

The comparison should identify differences in processing operations, execution
time and computational requirements.
Students should explain whether the observed differences arise from data size,
structural complexity, query design or other technical factors.

9. Submission Requirements
Each group must submit a five-minute YouTube video and a three-page PDF.
9.1 Video
The video must include:
1.  Contributions from all five group members.
2.  A clearly defined healthcare case study.
3.  A functional data processing prototype.
4.  Technical evidence addressing all five Vs.
5.  Analytical findings relevant to the selected healthcare problem.
6.  English or Bahasa Indonesia closed captions.
IFB-451 BIG DATA-Midterm Assignment-DrEdiNuryatno__2026                                     12 | P a g e

9.2 Three-Page PDF
Page Required Content
Page 1 — Project Case study title, group members, student numbers,
Information dataset description and source.
Page 2 — Video and YouTube link and a concise explanation of the
5Vs Summary evidence presented for each characteristic.
Page 3 — System Diagram showing data sources, ingestion, storage,
Architecture processing, integration, analysis and outputs.
Submission Platform
Itenas eLearning — IFB-451 BIG DATA
Submission Deadline
10 November 2026
File Naming Convention
GroupNumber_FullName_NIM.pdf
Each student must submit the group PDF individually using their respective name
and student number.
10. Assessment Rubric
The assessment evaluates conceptual understanding, technical implementation
and the interpretation of healthcare information.
Table 10.1. Assessment Criteria
Assessment Criterion SC Assessment Focus Weight
Volume SC1–SC3 Understanding of data scale, 10%
structural complexity and
processing requirements.
IFB-451 BIG DATA-Midterm Assignment-DrEdiNuryatno__2026 13 | P a g e

| Assessment Criterion  | SC       | Assessment Focus       | Weight  |
| --------------------- | -------- | ---------------------- | ------- |
| Velocity              | SC1–SC3  | Understanding of data  | 10%     |
arrival, ingestion and
processing timeliness.
| Variety  | SC1–SC2  | Integration and processing of  | 10%  |
| -------- | -------- | ------------------------------ | ---- |
different data formats and
representations.
| Veracity  | SC2–SC3  | Identification, validation and  | 10%  |
| --------- | -------- | ------------------------------- | ---- |
management of data quality
problems.
| Value  | SC3  | Analytical findings and their  | 15%  |
| ------ | ---- | ------------------------------ | ---- |
relevance to healthcare
understanding or decision-
making.
| Big Data Architecture  | SC1–SC3  | Appropriateness and          | 25%  |
| ---------------------- | -------- | ---------------------------- | ---- |
| and Technical          |          | functionality of the         |      |
| Implementation         |          | processing architecture and  |      |
prototype.
Healthcare Case Study  SC1–SC3  Relevance of the selected  10%
| and Interpretation  |     | problem and quality of  |     |
| ------------------- | --- | ----------------------- | --- |
analytical interpretation.
| Teamwork and  | All  | Balanced participation,       | 10%  |
| ------------- | ---- | ----------------------------- | ---- |
| Presentation  |      | clarity and adherence to the  |      |
five-minute limit.
|        |     |     |       |
| ------ | --- | --- | ----- |
| TOTAL  |     |     | 100%  |

Table 10.2. Assessment Distribution
| Component                  | Weight  |     |     |
| -------------------------- | ------- | --- | --- |
| Demonstration of the 5Vs   | 55%     |     |     |
| Technical Implementation   | 25%     |     |     |
| Healthcare Relevance       | 10%     |     |     |
| Teamwork and Presentation  | 10%     |     |     |
| TOTAL                      | 100%    |     |     |

IFB-451 BIG DATA-Midterm Assignment-DrEdiNuryatno__2026                                     14 | P a g e

11. Expected Learning Outcomes
Upon completing the assignment, students should be able to:
| Learning Outcome  | Competency  | Expected Achievement  |
| ----------------- | ----------- | --------------------- |
LO1 — Big Data  Conceptual  Explain the five characteristics of
Understanding  Understanding  Big Data and their implications for
healthcare information processing.
| LO2 — Data           | Data Analysis  | Examine healthcare datasets in  |
| -------------------- | -------------- | ------------------------------- |
| Characteristics and  |                | terms of scale, structure,      |
| Complexity           |                | relationships, processing       |
requirements and data quality.
| LO3 — Technical  | Applied    | Develop a functional data   |
| ---------------- | ---------- | --------------------------- |
| Implementation   | Computing  | processing prototype using  |
technologies appropriate to the
selected healthcare problem.
| LO4 — Data       | Data        | Integrate different data     |
| ---------------- | ----------- | ---------------------------- |
| Integration and  | Management  | representations and apply    |
| Quality          |             | appropriate data validation  |
procedures.
| LO5 — Analytical  | Healthcare  | Produce meaningful analytical  |
| ----------------- | ----------- | ------------------------------ |
Interpretation  Informatics  findings and explain their potential
contribution to healthcare
understanding or decision-making.
| LO6 — Technical  | Scientific     | Present the methodology,     |
| ---------------- | -------------- | ---------------------------- |
| Communication    | Communication  | implementation and findings  |
through a concise, evidence-
based technical demonstration.

12. Academic Requirements
Students must demonstrate an understanding of the technical decisions
underlying their implementation.
Table 12.1. Academic Requirements
| Requirement  | Description  |     |
| ------------ | ------------ | --- |
Data  Examine all five Vs in relation to the selected healthcare
| Characteristics  | dataset.  |     |
| ---------------- | --------- | --- |
IFB-451 BIG DATA-Midterm Assignment-DrEdiNuryatno__2026                                     15 | P a g e

Requirement Description
Technical Select processing technologies and architecture
Justification appropriate to the data and analytical requirements.
Computational Distinguish data size, structural complexity and
Considerations processing demands where relevant.
Data Quality Support validation and correction procedures with
measurable evidence.
Healthcare Relate analytical findings to the selected healthcare
Interpretation problem and acknowledge relevant limitations.
Data Protection Use public, synthetic or appropriately de-identified
datasets. Identifiable patient information must not be
disclosed.
Academic Integrity Present work that the group can explain, reproduce and
defend. External code, datasets and supporting
materials must be appropriately acknowledged.
The assessment does not reward system complexity for its own sake.
Students will be evaluated on their understanding of the selected problem, the
appropriateness of their methods and the evidence presented.
IFB-451 BIG DATA-Midterm Assignment-DrEdiNuryatno__2026 16 | P a g e
