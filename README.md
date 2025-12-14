# CSE452 — Big Data Analytics | IBA Karachi (Fall 2025)

![Course](https://img.shields.io/badge/Course-CSE452%20Big%20Data-0A66C2)
![Institute](https://img.shields.io/badge/Institute-IBA%20Karachi-111111)
![Semester](https://img.shields.io/badge/Semester-Fall%202025-2E7D32)
![Topics](https://img.shields.io/badge/Topics-Docker%20%7C%20NoSQL%20%7C%20Hadoop%20%7C%20Spark%20%7C%20Kafka%20%7C%20Hive%20%7C%20HBase%20%7C%20MongoDB-6A1B9A)

This repository contains my complete course resources for **CSE 452: Big Data Analytics** at **Institute of Business Administration (IBA), Karachi** (Fall 2025): lectures, labs, assignments, quizzes, datasets, and structured revision packs for mid and final.

Maintained by: **Zuha Aqib (IBA Karachi)**

---

## Table of Contents
- Course Information
- Course Scope (What this course covers)
- Grading Breakdown
- Weekly Breakdown (Tentative)
- Repository Structure
- Quick Links
- Labs Index
- Exam Preparation Packs
- Notes

---

## Course Information
- **Instructor:** Dr. Tariq Mahmood
- **Schedule:** Tue/Thu, **11:30am – 12:45pm**
- **Credits:** 3 (3+0)
- **Prerequisite:** CS341 Database Systems 

---

## Course Scope (What this course covers)
According to the official outline, the course targets a “360-degree” understanding of big data: how storage and analysis methods change in the presence of big data, with emphasis on **NoSQL storage/querying**, **interactive big data analytics**, **Docker-based containerization/orchestration**, and **data engineering pipelines**.

### Course Learning Outcomes (CLOs)
- **CLO1:** Big data storage based on NoSQL
- **CLO2:** Containerization-based BDA
- **CLO3:** Big data management, architectures, governance, security 
- **CLO4:** Data engineering knowledge & skills 

---

## Grading Breakdown (Tentative)
- **Quizzes:** 10% (5 quizzes, 2% each) 
- **Midterm:** 20%
- **Final Exam:** 30%
- **Assignments:** 15% (5 assignments, 3% each)
- **Project:** 25% (Video + Analytical Report)
- **Teams:** Teams of 2 allowed for assignments/activities/projects; decide early and continue to end.

---

## Weekly Breakdown (Tentative)
From the outline’s weekly plan:

- **Week 1:** Intro to Big Data (history, concepts, landscapes)
- **Weeks 2–3:** Distributed/parallel computing, virtualization, Docker (theory + hands-on), Linux commands + shell scripting
- **Weeks 4–5:** Document databases — **MongoDB** (theory + practical on Docker)
- **Weeks 6–9:** **Hadoop/HDFS**, **MapReduce**, **Spark**, **Kafka**, **Hive** (theory + practical on Docker)
- **Week 10:** Columnar warehouses — **HBase** (theory + practical on Docker)
- **Week 11:** Key-value databases — **Redis** (theory + practical on Docker)
- **Week 12:** Big data software architectures, AI-based analytics techniques, tools/trends
- **Weeks 13–14:** **Data Engineering** + end-to-end data pipeline, best practices, relation to DevOps, CI/CD data pipeline using **Apache Airflow** and related tools
- **Week 15:** Big Data and AI

---

## Repository Structure
```text
.
├── Fall25_CS452_BDA_Outline.pdf
├── assignments/
├── books/
├── chatgpt-questions/
├── class-notes/
├── datasets/
├── final-exam/
├── labs/
│   ├── handout01_installation/
│   ├── handout02_linuxScripting/
│   ├── handout03_dockerContainers/
│   ├── handout04_hadoopSingle/
│   ├── handout05_hadoopMulti/
│   ├── handout05_mapReduce/
│   ├── handout06_hive/
│   ├── handout06_sparkAndKafka/
│   ├── handout07_hbase/
│   ├── handout08_mongo/
│   └── revision_notes/
├── lectures/
├── quizzes/
├── revision-notes/
│   ├── final-notes/
│   ├── lecture-notes/
│   ├── lecture-practice/
│   └── mid-notes/
└── virtual-machine/
```

---

## Quick Links

* **Course Outline:** `Fall25_CS452_BDA_Outline.pdf`
* **Lectures:** `lectures/`
* **Labs:** `labs/`
* **Assignments:** `assignments/`
* **Quizzes:** `quizzes/`
* **Revision Notes (mid + final):** `revision-notes/`
* **Final Exam Tips:** `final-exam/final-exam.txt`
* **Datasets / Sources:** `datasets/BigDataSources.txt`

---

## Labs Index

Labs are organized in the same flow as the course’s tooling stack (Docker → Linux → Hadoop → Hive/Spark/Kafka → HBase → MongoDB):

1. `labs/handout01_installation/` — Docker Desktop (Windows) + Docker on Ubuntu
2. `labs/handout02_linuxScripting/` — Linux commands & shell scripting
3. `labs/handout03_dockerContainers/` — Images/containers/volumes/compose
4. `labs/handout04_hadoopSingle/` — Single-node Hadoop (includes sample output)
5. `labs/handout05_mapReduce/` — MapReduce detailed handouts
6. `labs/handout05_hadoopMulti/` — Multi-cluster Hadoop lab
7. `labs/handout06_hive/` — Hive lab + datasets + compose variants
8. `labs/handout06_sparkAndKafka/` — Spark + Kafka activities

   * Producers/consumers/streaming scripts
   * Dashboard template in `templates/`
   * Outputs saved in `outputs/`
9. `labs/handout07_hbase/` — HBase lab + data files
10. `labs/handout08_mongo/` — MongoDB clustering/sharding (single + cluster compose + init scripts)

Lab revision pack (upto mid): `labs/revision_notes/upto_mid_labs.pdf`

---

## Exam Preparation Packs

### Midterm Pack

Folder: `revision-notes/mid-notes/`

* Cheatsheet, lab revision, mock exams, revision sets, and compiled notes.

Suggested order:

1. `mid_cheatsheet.pdf`
2. `mid_labs_revision.pdf`
3. `mid_revision_1.pdf` → `mid_revision_2.pdf`
4. `mid_mock_exam_1.pdf` → `mid_mock_exam_2.pdf`

### Final Pack

Folder: `revision-notes/final-notes/`

* Topic-wise notes: Hive, Spark, Kafka, HBase, MongoDB, Lambda architecture, plus full revision sets.

Suggested order:

1. Topic-wise PDFs (Hive → Spark → Kafka → HBase → MongoDB)
2. `Lambda-Architecture-Q.pdf`
3. `full-revision-1.pdf` → `full-revision-2.pdf`
4. `final-exam/final-exam.txt` (quick tips)

---

## Notes

* The LMS is used for sharing syllabus/resources and submissions. 
* University policies on attendance and academic integrity apply. 
* Late submission policy (as listed in outline): up to 1 day late accepted with 10% penalty; beyond that, not accepted. 
* Office hours (outline): Monday & Wednesday, 4:30pm–5:15pm. 